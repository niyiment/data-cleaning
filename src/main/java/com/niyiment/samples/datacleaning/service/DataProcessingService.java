package com.niyiment.samples.datacleaning.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.niyiment.samples.datacleaning.dto.CleanedDataResult;
import com.niyiment.samples.datacleaning.exception.ReportProcessingException;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class DataProcessingService {
    private final ObjectMapper objectMapper;

    public CleanedDataResult processFile(MultipartFile file) {
        log.debug("Initializing data cleaning process");

        String filename = file.getOriginalFilename();
        String fileExtension = getFileExtension(filename);

        List<Map<String, Object>> rawData;
        switch (fileExtension.toLowerCase()) {
            case "csv" -> rawData = processCSV(file);
            case "xlsx" -> rawData = processExcel(file);
            case "json" -> rawData = processJSON(file);
            default -> throw new ReportProcessingException("Unsupported file format: " + fileExtension);
        }

       return toCleanedData(rawData);
    }

    public List<Map<String, Object>> processCSV(MultipartFile file) {
        log.debug("Processing CSV file");

        List<Map<String, Object>> data = new ArrayList<>();
        try (Reader reader = new InputStreamReader(file.getInputStream());
         CSVReader csvReader = new CSVReader(reader)) {
            String[] headers = csvReader.readNext();
            String[] lines;
            while ((lines = csvReader.readNext()) != null) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    row.put(headers[i], lines[i]);
                }
                data.add(row);
            }
        } catch (CsvValidationException | IOException e) {
            throw new ReportProcessingException("Error processing CSV file", e);
        }

        return data;
    }

    public List<Map<String, Object>> processExcel(MultipartFile file) {
        log.debug("Processing Excel file");

        List<Map<String, Object>> data = new ArrayList<>();
        try(Workbook workbook =  new XSSFWorkbook(file.getInputStream())){
            Sheet sheet = workbook.getSheetAt(0);
            Row headerRow = sheet.getRow(0);

            List<String> headers = new ArrayList<>();
            for (Cell cell : headerRow) {
                headers.add(cell.getStringCellValue());
            }

            for (int rowNumber=0; rowNumber < sheet.getLastRowNum(); rowNumber++) {
                Row row = sheet.getRow(rowNumber);
                Map<String, Object> rowData = new HashMap<>();
                for (int columnNumber=0; columnNumber < headers.size(); columnNumber++) {
                    Cell cell = row.getCell(columnNumber, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                    rowData.put(headers.get(columnNumber), getCellValue(cell));
                }
                data.add(rowData);
            }

        } catch (IOException e) {
            throw new ReportProcessingException("Error processing Excel file", e);
        }

        return data;
    }

    public List<Map<String, Object>> processJSON(MultipartFile file) {
        log.debug("Processing JSON file");

        List<Map<String, Object>> data = new ArrayList<>();
        try{
            data = objectMapper.readValue(file.getInputStream(),
                    objectMapper.getTypeFactory().constructCollectionType(List.class, Map.class));

        } catch (IOException e) {
            throw new ReportProcessingException("Error processing JSON file", e);
        }
        return data;
    }

    private String getFileExtension(String filename) {
        return filename.substring(filename.lastIndexOf('.') + 1);
    }

    private Object getCellValue(Cell cell) {
        if (cell == null) {
            return null;
        }

        return switch (cell.getCellType()) {
            case NUMERIC -> {
                if (DateUtil.isCellDateFormatted(cell)) {
                    yield cell.getDateCellValue();
                }
                yield cell.getNumericCellValue();
            }
            case STRING -> cell.getStringCellValue();
            case BOOLEAN -> cell.getBooleanCellValue();
            case FORMULA -> cell.getCellFormula();
            default -> null;
        };
    }

    private CleanedDataResult toCleanedData(List<Map<String, Object>> data) {
        return CleanedDataResult.builder()
                .cleanedData(data)
                .dataQueryReport(null)
                .columns(null)
                .build();
    }
}
