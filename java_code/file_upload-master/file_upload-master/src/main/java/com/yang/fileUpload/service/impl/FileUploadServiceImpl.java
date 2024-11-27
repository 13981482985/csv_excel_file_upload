package com.yang.fileUpload.service.impl;
import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.yang.fileUpload.mapper.FileMapper;
import com.yang.fileUpload.service.FileUploadService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.yang.fileUpload.utils.FileUtil.*;

@Slf4j
@Service
public class FileUploadServiceImpl  implements FileUploadService {


    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;
    @Autowired
    private FileMapper fileMapper;

    private static final Integer BATCH_SIZE = 500;

    private AtomicBoolean tableIsCreate = new AtomicBoolean(false);


    /**
     *
     * @param csvFile 上传的csv文件
     * @param tableName 文件名称也是数据库表名
     * @throws IOException
     * @throws CsvValidationException
     */
    @Override
    public void csvUpload(File csvFile, String tableName) throws IOException, CsvValidationException {
        FileReader fileReader = new FileReader(csvFile);
        CSVReader csvReader = new CSVReaderBuilder(fileReader).build();
        tableName = tableName.replace(" ", "").replace("-", "_");
        String[] colNames = readHeaders(csvReader);
        // 1. 读取数据用于类型推断
        List<String[]> rows = readAllRows(csvReader);
        // 2. 生成列定义 key列名 value类型
        LinkedHashMap<String, String> columnDefinitions = generateColumnDefinitions(colNames, rows);
        // 3. 创建表结构
        fileMapper.createTable(tableName, columnDefinitions);
        // 4. 加载表格数据
        String filePath = csvFile.getAbsolutePath().replace("\\", "/");
        fileMapper.loadData(filePath, tableName);
    }

    /**
     * 多线程数据文件上传 VS 数据库 load 命令直接导入数据
     */

    @Override
    public void martiThreadUpload(File csvFile, String fileName){
        String[] colNames = null;
        List<String[]> rows = null;
        String tableName = fileName.replace(" ", "").replace("-", "_");
        try {
            FileReader fileReader = new FileReader(csvFile);
            CSVReader csvReader = new CSVReaderBuilder(fileReader).build();
            colNames = readHeaders(csvReader);
            // 1. 读取数据用于类型推断
            rows = readAllRows(csvReader);
        }catch (Exception e){
            log.error("文件读取错误：{}", csvFile.getAbsolutePath());
        }
        // 2. 生成列定义 key列名 value类型
        LinkedHashMap<String, String> columnDefinitions = generateColumnDefinitions(colNames, rows);
        // 3. 创建表结构
        fileMapper.createTable(tableName, columnDefinitions);
        // 4. 加载数据
        int totalBatches = (int) Math.ceil((double) rows.size() / BATCH_SIZE);
        List<String[]> finalRows = rows;
        List<CompletableFuture<Void>> futures = IntStream.range(0, totalBatches)
                .mapToObj(i -> {
                    int start = i * BATCH_SIZE;
                    int end = Math.min(start + BATCH_SIZE, finalRows.size());
                    List<String[]> batchData = finalRows.subList(start, end);
                    return CompletableFuture.runAsync(() -> {
                        try {
                            fileMapper.batchInsert(tableName, batchData);
                        }catch (Exception e){
                            log.error("Error processing batch {} to {} : {}", start, end, e.getMessage());
                        }finally {
                            fileMapper.deleteTable(tableName);
                        }
                    }, threadPoolExecutor);
                }).collect(Collectors.toList());
        // 等待所有批次完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }


    @Override
    public void completableFutureUploadCsv(File csvFile, String fileName){
        String tableName = fileName.replace(" ", "").replace("-", "_");
        CompletableFuture<Map.Entry<LinkedHashMap<String, String>, List<String[]>>> colDefinitionFuture = CompletableFuture.supplyAsync(() -> {
            try {
                // 读取表头 获取列名
                FileReader fileReader = new FileReader(csvFile);
                CSVReader csvReader = new CSVReaderBuilder(fileReader).build();
                String[] colNames = readHeaders(csvReader);
                // 1. 读取数据用于类型推断
                List<String[]> rows = readAllRows(csvReader);
                // 2. 生成列定义 key列名 value类型
                LinkedHashMap<String, String> columnDefinitions = generateColumnDefinitions(colNames, rows);
                return new AbstractMap.SimpleEntry<>(columnDefinitions, rows);
                // 读取数据 推断列类型
            } catch (Exception e) {
                throw new RuntimeException("文件读取异常："+fileName);
            }
        }, threadPoolExecutor);
        // 新建表 colDefinition 是 colDefinitionFuture 的返回结果
        CompletableFuture<List<String[]>> createTableFuture = colDefinitionFuture.thenComposeAsync(colDefinition -> {
            try {
                fileMapper.createTable(tableName, colDefinition.getKey());
                return CompletableFuture.completedFuture(colDefinition.getValue());
            } catch (Exception e) {
                throw new RuntimeException("create Table "+tableName+" error!");
            }
        }, threadPoolExecutor);

        CompletableFuture<Void> loadDataFuture = createTableFuture.thenComposeAsync(rows -> {
            int epoch = (int) Math.ceil((double) rows.size() / BATCH_SIZE);
            List<CompletableFuture<Void>> alLoadDataFuture = IntStream.range(0, epoch).mapToObj(i -> {
                int start = BATCH_SIZE * i;
                int end = Math.min(BATCH_SIZE * (i + 1), rows.size());
                CompletableFuture<Void> insertFuture = CompletableFuture.runAsync(() -> {
                    try{
                        fileMapper.batchInsert(tableName, rows.subList(start, end));
                    }catch (Exception e){
                        log.error("批次插入失败，范围 {} 到 {}：{}", start, end, e.getMessage());
                    }
                }, threadPoolExecutor);
                return insertFuture;
            }).collect(Collectors.toList());
            // 等待所有批量插入任务完成
            return CompletableFuture.allOf(alLoadDataFuture.toArray(new CompletableFuture[0]));
        }, threadPoolExecutor);
        try {
            loadDataFuture.join();
        }catch (Exception e){
            log.error("文件上传过程中发生错误，删除表：{}", tableName, e);
            fileMapper.deleteTable(tableName);
        }
    }

    @Override
    public void completableFutureUploadExcel(File toFile, String fileName) {
        String tableName = fileName.replace(" ", "").replace("-", "_");
         /**
          * 1. 读取 Excel 文件，返回表头和数据
         * doRead() 是执行数据读取的核心方法。
         *  当配置好 sheet() 和 headRowNumber() 后，调用 doRead() 会开始解析 Excel 数据。
         *  数据逐行传递给 AnalysisEventListener 的 invoke() 方法。
         */
         try {
             EasyExcel.read(toFile, new AnalysisEventListener<Map<Integer, String>>() {
                 List<String []> allRowData = new ArrayList<>();
                 String[] colNames;
                 List<CompletableFuture> futures = new ArrayList<>();
                 @Override
                 public void invokeHeadMap(Map<Integer, String> headMap, AnalysisContext context) {
                     // 解析标题
                     colNames = new String[headMap.size()];
                     for (Map.Entry<Integer, String> head : headMap.entrySet()) {
                         colNames[head.getKey()] = head.getValue();
                     }
                 }
                 @Override
                 public void invoke(Map<Integer, String> row, AnalysisContext analysisContext) {
                     String[] rowData = new String[colNames.length];
                     for (Map.Entry<Integer, String> filed : row.entrySet()) {
                         rowData[filed.getKey()] = filed.getValue();
                     }
                     allRowData.add(rowData);
                     if(allRowData.size() >= BATCH_SIZE){
                         List<String[]> curBatchData = new ArrayList<>();
                         curBatchData.addAll(allRowData);
                         allRowData.clear();
                         if(tableIsCreate.compareAndSet(false, true)){
                             CompletableFuture<Void> inertFuture = CompletableFuture.runAsync(() -> {
                                 LinkedHashMap<String, String> columnDefinition = generateColumnDefinitions(colNames, curBatchData);
                                 fileMapper.createTable(tableName, columnDefinition);
                             }, threadPoolExecutor);
                             futures.add(inertFuture);
                         }else{
                             CompletableFuture<Void> inertFuture = CompletableFuture.runAsync(() -> {
                                 fileMapper.batchInsert(tableName, curBatchData);
                             }, threadPoolExecutor);
                             futures.add(inertFuture);
                         }
                     }
                 }
                 @Override
                 public void doAfterAllAnalysed(AnalysisContext analysisContext) {
                     if(!allRowData.isEmpty()){
                         CompletableFuture<Void> insertFuture = CompletableFuture.runAsync(() -> {
                             if(tableIsCreate.compareAndSet(false, true)){
                                 LinkedHashMap<String, String> columnDefinition = generateColumnDefinitions(colNames, allRowData);
                                 fileMapper.createTable(tableName, columnDefinition);
                             }
                             fileMapper.batchInsert(tableName, allRowData);
                         }, threadPoolExecutor);
                         futures.add(insertFuture);
                     }
                     // 等待所有线程执行完毕
                     CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                 }
             }).sheet().headRowNumber(1).doRead(); // 设置标题行为第一行
         }catch (Exception e){
             fileMapper.deleteTable(tableName);
             tableIsCreate.compareAndSet(true, false);
             throw new RuntimeException(e.getMessage());
         }
    }


    /**
     * 采用单线程方式上传，比较效果
     * @param toFile
     * @param fileName
     */

    public void completableFutureUploadExcel2(File toFile, String fileName) {
        String tableName = fileName.replace(" ", "").replace("-", "_");
        /**
         * 1. 读取 Excel 文件，返回表头和数据
         * doRead() 是执行数据读取的核心方法。
         *  当配置好 sheet() 和 headRowNumber() 后，调用 doRead() 会开始解析 Excel 数据。
         *  数据逐行传递给 AnalysisEventListener 的 invoke() 方法。
         */
        try {
            EasyExcel.read(toFile, new AnalysisEventListener<Map<Integer, String>>() {
                //            volatile Boolean tableIsCreate = false;
                List<String []> allRowData = new ArrayList<>();
                String[] colNames;
                List<CompletableFuture> futures = new ArrayList<>();
                @Override
                public void invokeHeadMap(Map<Integer, String> headMap, AnalysisContext context) {
                    // 解析标题
                    colNames = new String[headMap.size()];
                    for (Map.Entry<Integer, String> head : headMap.entrySet()) {
                        colNames[head.getKey()] = head.getValue();
                    }
                }
                @Override
                public void invoke(Map<Integer, String> row, AnalysisContext analysisContext) {
                    String[] rowData = new String[colNames.length];
                    for (Map.Entry<Integer, String> filed : row.entrySet()) {
                        rowData[filed.getKey()] = filed.getValue();
                    }
                    allRowData.add(rowData);
                    if(allRowData.size() >= BATCH_SIZE){
                        List<String[]> curBatchData = new ArrayList<>();
                        curBatchData.addAll(allRowData);
                        allRowData.clear();
                        if(tableIsCreate.compareAndSet(false, true)){
                            LinkedHashMap<String, String> columnDefinition = generateColumnDefinitions(colNames, curBatchData);
                            fileMapper.createTable(tableName, columnDefinition);
                        }else{
                            fileMapper.batchInsert(tableName, curBatchData);
                        }
                    }
                }
                @Override
                public void doAfterAllAnalysed(AnalysisContext analysisContext) {
                    if(!allRowData.isEmpty()){
                        if(tableIsCreate.compareAndSet(false, true)){
                            LinkedHashMap<String, String> columnDefinition = generateColumnDefinitions(colNames, allRowData);
                            fileMapper.createTable(tableName, columnDefinition);
                        }
                        fileMapper.batchInsert(tableName, allRowData);
                    }
                    tableIsCreate.compareAndSet(true, false);
                }
            }).sheet().headRowNumber(1).doRead(); // 设置标题行为第一行
        }catch (Exception e){
            fileMapper.deleteTable(tableName);
            tableIsCreate.compareAndSet(true, false);
            throw new RuntimeException(e.getMessage());
        }

    }
}
