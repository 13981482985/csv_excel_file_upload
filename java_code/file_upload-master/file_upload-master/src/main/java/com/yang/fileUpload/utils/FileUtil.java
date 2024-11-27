package com.yang.fileUpload.utils;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class FileUtil {

    // 读取表头
    public static String[] readHeaders(CSVReader csvReader) throws IOException, CsvValidationException {
        String[] headers = csvReader.readNext();
        if (headers == null) {
            throw new IllegalArgumentException("CSV 文件为空！");
        }
        return headers;
    }

    // 读取所有行
    public static List<String[]> readAllRows(CSVReader csvReader) throws IOException, CsvValidationException {
        List<String[]> rows = new ArrayList<>();
        String[] row;
        while ((row = csvReader.readNext()) != null) {
            rows.add(row);
        }
        return rows;
    }

    // 生成列定义
    public static LinkedHashMap<String,String> generateColumnDefinitions(String[] headers, List<String[]> rows) {
        LinkedHashMap<String,String> map = new LinkedHashMap<>();
        for (int i = 0; i < headers.length; i++) {
            String columnName = headers[i].replace("\\s+", "_");
            String columnType = inferColumnType(rows, i);
            columnName = columnName
                    .replace("-", "_")
                    .replace("\"", "")
                    .replace(" ", "")
                    .replace("(", "")
                    .replace(")", "");
            map.put(columnName, columnType);
        }
        return map;
    }
    // 推断列的类型

    private static final Pattern DATE_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");
    private static final Pattern DATETIME_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$");

    public static String inferColumnType(List<String[]> rows, int columnIndex) {
        boolean isBigInt = true;
        boolean isInteger = true;
        boolean isDouble = true;
        boolean isDate = true;
        boolean isDateTime = true;

        for (String[] row : rows) {
            if (row.length <= columnIndex) continue; // 跳过短行
            String value = row[columnIndex];
            if (value == null || value.isEmpty()) continue; // 跳过空值

            if (isBigInt && !isBigInt(value)) {
                isBigInt = false;
            }
            if (isInteger && !isInteger(value)) {
                isInteger = false;
            }
            if (isDouble && !isDouble(value)) {
                isDouble = false;
            }
            if (isDate && !DATE_PATTERN.matcher(value).matches()) {
                isDate = false;
            }
            if (isDateTime && !DATETIME_PATTERN.matcher(value).matches()) {
                isDateTime = false;
            }
        }

        if (isBigInt) return "BIGINT";
        if (isInteger) return "INT(32)";
        if (isDouble) return "DOUBLE(64, 2)";
        if (isDate) return "DATE";
        if (isDateTime) return "DATETIME";

        return "VARCHAR(255)";
    }

    private static boolean isBigInt(String value) {
        try {
            long parsedValue = Long.parseLong(value); // BIGINT 在 Java 中对应 long
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }


    private static boolean isInteger(String value) {
        try {
            long longValue = Long.parseLong(value);
            return longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    private static boolean isDouble(String value) {
        try {
            double doubleValue = Double.parseDouble(value);
            return Double.isFinite(doubleValue);
        } catch (NumberFormatException e) {
            return false;
        }
    }


    /**
     * 读取 Excel 文件并生成表头和数据
     */
    public static String[] readExcelAndGenerateData(String excelFilePath, List<String[]> rows) {
        // 存储表头信息
        final List<String> headers = new ArrayList<>();

        EasyExcel.read(excelFilePath, new AnalysisEventListener<Map<Integer, String>>() {

            @Override
            public void invokeHeadMap(Map<Integer, String> headMap, AnalysisContext context) {
                // 保存表头信息
                headers.addAll(headMap.values());
            }

            @Override
            public void invoke(Map<Integer, String> data, AnalysisContext context) {
                // 将每行数据转换为 String[]
                String[] row = new String[headers.size()];
                for (Map.Entry<Integer, String> entry : data.entrySet()) {
                    row[entry.getKey()] = entry.getValue();
                }
                rows.add(row); // 添加到行列表
//                if(rows.size()>)
            }
            @Override
            public void doAfterAllAnalysed(AnalysisContext context) {
                System.out.println("Excel 数据解析完成！");
            }
        }).sheet().headRowNumber(1).doRead(); // 设置表头行数为 1

        return headers.toArray(new String[0]);
    }




}
