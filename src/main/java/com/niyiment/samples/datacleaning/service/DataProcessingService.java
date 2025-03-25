package com.niyiment.samples.datacleaning.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.niyiment.samples.datacleaning.dto.CleanedDataResult;
import com.niyiment.samples.datacleaning.dto.DataQualityReport;
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
import java.util.*;
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

       return cleanAndAnalyzeData(rawData);
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
                    row.put(standardizeColumnName(headers[i]), i < lines.length ? lines[i] : null);
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
                headers.add(standardizeColumnName(cell.getStringCellValue()));
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

            return data.stream().map(this::standardizeRowKeys).collect(Collectors.toList());
        } catch (IOException e) {
            throw new ReportProcessingException("Error processing JSON file", e);
        }
    }

    private CleanedDataResult cleanAndAnalyzeData(List<Map<String, Object>> data) {
        List<Map<String, Object>> cleanedData = data.stream()
                .map(this::removeSpecialCharacters)
                .map(this::normalizeWhitespace)
                .map(this::handleMissingValues)
                .toList();

        DataQualityReport report = generateDataQualityReport(data, cleanedData);

        return CleanedDataResult.builder()
                .cleanedData(cleanedData)
                .dataQueryReport(report)
                .columns(new ArrayList<>(cleanedData.get(0).keySet()))
                .build();
    }

    private Map<String, Object> standardizeRowKeys(Map<String, Object> row) {
        return row.entrySet().stream()
                .collect(Collectors.toMap(entry -> standardizeColumnName(entry.getKey()),
                        Map.Entry::getValue));
    }

    private String standardizeColumnName(String columnName) {
        return columnName
                .trim()
                .toLowerCase()
                .replaceAll("[^a-z0-9]+", "_");
    }

    private Map<String, Object> removeSpecialCharacters(Map<String, Object> row) {
        return row.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            if (entry.getValue() instanceof String) {
                                return ((String) entry.getValue())
                                        .replaceAll("[^a-zA-Z0-9\\s]", "");
                            }

                            return entry.getValue();
                        }
                ));
    }

    private Map<String, Object> normalizeWhitespace(Map<String, Object> row) {
        return row.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                        entry -> {
                            if (entry.getValue() instanceof String) {
                                return ((String) entry.getValue())
                                       .replaceAll("\\s+", " ");
                            }

                            return entry.getValue();
                        }
                ));
    }

    private Map<String, Object> handleMissingValues(Map<String, Object> row) {
        return row.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            Object value = entry.getValue();
                            if (value == null ||
                                    (value instanceof String &&
                                            ((String) value).trim().isEmpty())) {
                                return "N/A";
                            }
                            return value;
                        }
                ));
    }

    private DataQualityReport generateDataQualityReport(List<Map<String, Object>> rawData,
                                                        List<Map<String, Object>> cleanedData) {
        Map<String, Long> missingValuesCount = rawData.stream()
                .flatMap(row -> row.entrySet().stream())
                .filter(entry -> entry.getValue() == null ||
                        (entry.getValue() instanceof String && ((String) entry.getValue()).trim().isEmpty()))
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.counting()));

        Map<String, Integer> uniqueValuesCount = cleanedData.stream()
                .flatMap(row -> row.entrySet().stream())
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.collectingAndThen(
                        Collectors.mapping(Map.Entry::getValue, Collectors.toSet()),
                        Set::size)));

        return DataQualityReport.builder()
                .totalRecords(rawData.size())
                .processedRecords(cleanedData.size())
                .missingValuesCount(missingValuesCount)
                .uniqueValuesCount(uniqueValuesCount)
                .build();

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

}
