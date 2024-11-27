package com.yang.fileUpload.service;

import com.opencsv.exceptions.CsvValidationException;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;

public interface FileUploadService {

    void csvUpload(File csvFile, String tableName) throws IOException, CsvValidationException;

    void martiThreadUpload(File csvFile, String fileName);

    void completableFutureUploadCsv(File csvFile, String fileName);

    void completableFutureUploadExcel(File toFile, String filename);

    void completableFutureUploadExcel2(File toFile, String fileName);

}
