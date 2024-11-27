package com.yang.fileUpload.utils;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ExcelReader {

    private String[] colNames; // 保存表头
    private List<String[]> rows = new ArrayList<>(); // 保存数据行
    public ExcelReader(File excelFile) {
        EasyExcel.read(excelFile, new AnalysisEventListener<List<String>>() {
            private boolean isHeaderProcessed = false; // 是否已经处理表头
            @Override
            public void invoke(List<String> row, AnalysisContext context) {
                if (!isHeaderProcessed) {
                    // 第一行作为表头
                    colNames = row.toArray(new String[0]);
                    isHeaderProcessed = true;
                } else {
                    // 其他行作为数据
                    rows.add(row.toArray(new String[0]));
                }
            }
            @Override
            public void doAfterAllAnalysed(AnalysisContext context) {}
        }).sheet().doRead();
    }
}