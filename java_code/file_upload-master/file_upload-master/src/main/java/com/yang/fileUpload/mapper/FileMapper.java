package com.yang.fileUpload.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Mapper
public interface FileMapper {
    void createTable(@Param("tableName")String tableName, @Param("colDefinition") LinkedHashMap<String, String> colDefinition);

    void loadData(@Param("filePath") String absolutePath, @Param("tableName") String tableName);

    void batchInsert(@Param("tableName") String finalTableName, @Param("rowData") List<String[]> batchData);

    void deleteTable(@Param("tableName") String tableName);
}
