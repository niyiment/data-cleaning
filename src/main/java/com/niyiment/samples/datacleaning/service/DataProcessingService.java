package com.niyiment.samples.datacleaning.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.niyiment.samples.datacleaning.dto.CleanedDataResult;
import com.niyiment.samples.datacleaning.dto.DataQualityReport;
import com.niyiment.samples.datacleaning.exception.ReportProcessingException;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.output.ByteArrayOutputStream;
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
import java.util.stream.StreamSupport;

@Service
@Slf4j
@RequiredArgsConstructor
public class DataProcessingService {
    private final ObjectMapper objectMapper;

    public CleanedDataResult processFile(MultipartFile file) {
        log.debug("Initializing data cleaning process");

        validateFileInput(file);
        String filename = Optional.ofNullable(file.getOriginalFilename())
                .orElseThrow(() -> new ReportProcessingException("File name cannot be null"));
        String fileExtension = getFileExtension(filename);

        List<Map<String, Object>> rawData;
        try {
            switch (fileExtension.toLowerCase()) {
                case "csv" -> rawData = processCSV(file);
                case "xlsx" -> rawData = processExcel(file);
                case "json" -> rawData = processJSON(file);
                default -> throw new ReportProcessingException("Unsupported file format: " + fileExtension);
            }
        } catch (Exception e) {
            log.error("Error processing file: {}", e.getMessage());
            throw new ReportProcessingException("Error processing file: " + e.getMessage(), e);
        }

        if (rawData == null || rawData.isEmpty()) {
            throw new ReportProcessingException("No data found in the file");
        }

       return cleanAndAnalyzeData(rawData);
    }

    public List<Map<String, Object>> processCSV(MultipartFile file) {
        log.debug("Processing CSV file");

        List<Map<String, Object>> data = new ArrayList<>();
        try (Reader reader = new InputStreamReader(file.getInputStream());
         CSVReader csvReader = new CSVReader(reader)) {
            String[] headers = Optional.ofNullable(csvReader.readNext())
                    .orElseThrow(() -> new ReportProcessingException("CSV file is empty"));
            List<String> cleanedHeaders = Arrays.stream(headers)
                    .map(this::standardizeColumnName)
                    .toList();
            String[] lines;

            while ((lines = csvReader.readNext()) != null) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 0; i < cleanedHeaders.size(); i++) {
                    String value = i < lines.length ? lines[i] : "";
                    row.put(standardizeColumnName(cleanedHeaders.get(i)), value);
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
            Sheet sheet = Optional.ofNullable(workbook.getSheetAt(0))
                    .orElseThrow(() -> new ReportProcessingException("No sheets found in the Excel file"));

            Row headerRow = Optional.ofNullable(sheet.getRow(0))
                    .orElseThrow(() -> new ReportProcessingException("No rows found in the Excel file"));

            List<String> headers = StreamSupport.stream(headerRow.spliterator(), false)
                    .map(cell -> {
                        try {
                            return standardizeColumnName(Optional.ofNullable(cell).map(Cell::getStringCellValue).orElse(""));
                        } catch (Exception e) {
                            return "column_" + headerRow.getCell(cell.getColumnIndex()).getColumnIndex();
                        }
                    })
                    .toList();

            for (int rowNumber=1; rowNumber <= sheet.getLastRowNum(); rowNumber++) {
                Row row = sheet.getRow(rowNumber);
                if (row == null) continue;

                Map<String, Object> rowData = new HashMap<>();
                boolean hasNonEmptyValue = false;
                for (int columnNumber=0; columnNumber < headers.size(); columnNumber++) {
                    Cell cell = row.getCell(columnNumber, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);

                    Object cellValue = safeCellValueExtraction(cell);

                    if (cellValue != null &&
                            !(cellValue instanceof String &&
                                    (((String) cellValue).trim().isEmpty() ||
                                            ((String) cellValue).equalsIgnoreCase("null")))) {
                        hasNonEmptyValue = true;
                    }

                    rowData.put(headers.get(columnNumber),
                            cellValue == null ? "N/A" : cellValue);
                }

                if (hasNonEmptyValue) {
                    data.add(rowData);

                }
            }

        } catch (IOException e) {
            throw new ReportProcessingException("Error processing Excel file", e);
        }

        return data;
    }

    public List<Map<String, Object>> processJSON(MultipartFile file) {
        log.debug("Processing JSON file");

        try{
            List<Map<String, Object>> data = objectMapper.readValue(file.getInputStream(),
                    objectMapper.getTypeFactory().constructCollectionType(List.class, Map.class));

            return Optional.ofNullable(data)
                    .orElse(Collections.emptyList())
                    .stream()
                    .filter(row-> row != null && !row.isEmpty())
                    .map(this::standardizeRowKeys)
                    .toList();
        } catch (IOException e) {
            throw new ReportProcessingException("Error processing JSON file", e);
        }
    }

    public byte[] exportToExcel(CleanedDataResult cleanedDataResult) throws IOException {
        try (Workbook workbook = new XSSFWorkbook()) {
            log.debug("Exporting data to excel, size: {}", cleanedDataResult.cleanedData().size());
            Sheet sheet = workbook.createSheet("Cleaned Data");

            // Create header row
            Row headerRow = sheet.createRow(0);
            List<String> columns = cleanedDataResult.columns();
            for (int i = 0; i < columns.size(); i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(columns.get(i));
            }

            // Populate data rows
            List<Map<String, Object>> cleanedData = cleanedDataResult.cleanedData();
            for (int rowNum = 0; rowNum < cleanedData.size(); rowNum++) {
                Row row = sheet.createRow(rowNum + 1);
                Map<String, Object> dataRow = cleanedData.get(rowNum);

                for (int colNum = 0; colNum < columns.size(); colNum++) {
                    Cell cell = row.createCell(colNum);
                    Object value = dataRow.get(columns.get(colNum));

                    if (value != null) {
                        if (value instanceof String) {
                            cell.setCellValue((String) value);
                        } else if (value instanceof Number) {
                            cell.setCellValue(((Number) value).doubleValue());
                        } else if (value instanceof Boolean) {
                            cell.setCellValue((Boolean) value);
                        } else {
                            cell.setCellValue(value.toString());
                        }
                    }
                }
            }

            // Convert workbook to byte array
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            workbook.write(outputStream);
            return outputStream.toByteArray();
        }
    }

    private void validateFileInput(MultipartFile file) {
        if (file == null) {
            throw new ReportProcessingException("File cannot be null");
        }
        if (file.isEmpty()) {
            throw new ReportProcessingException("File is empty");
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
                .dataQualityReport(report)
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
                                            (((String) value).trim().isEmpty() ||
                                                    ((String) value).equalsIgnoreCase("null")))) {
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

        Integer totalRecords = rawData.size();

        return DataQualityReport.builder()
                .totalRecords(totalRecords)
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

        try {
            return switch (cell.getCellType()) {
                case NUMERIC -> {
                    if (DateUtil.isCellDateFormatted(cell)) {
                        yield cell.getDateCellValue();
                    }
                    yield cell.getNumericCellValue();
                }
                case STRING -> {
                    String stringValue = cell.getStringCellValue();
                    yield stringValue != null ? stringValue : "";
                }
                case BOOLEAN -> cell.getBooleanCellValue();
                case FORMULA -> {
                    try{
                        yield cell.getStringCellValue();
                    } catch (Exception e) {
                        yield cell.getCellFormula();
                    }
                }
                default -> null;
            };
        } catch (Exception e) {
            log.warn("Error getting cell value: {}", e.getMessage());
            return null;
        }
    }

    private String safeGetCellStringValue(Cell cell) {
        if (cell == null) return "";

        try {
            // Try different methods to get string value
            switch (cell.getCellType()) {
                case STRING -> {
                    String value = cell.getStringCellValue();
                    return value != null ? value.trim() : "";
                }
                case NUMERIC -> {
                    // Handle numeric cells that might represent text
                    if (DateUtil.isCellDateFormatted(cell)) {
                        return cell.getDateCellValue().toString();
                    }
                    // Convert numeric to string, removing .0 for whole numbers
                    double numValue = cell.getNumericCellValue();
                    return numValue == (long) numValue
                            ? String.valueOf((long) numValue)
                            : String.valueOf(numValue);
                }
                case BOOLEAN -> {
                    return String.valueOf(cell.getBooleanCellValue());
                }
                case FORMULA -> {
                    FormulaEvaluator evaluator = cell.getSheet().getWorkbook().getCreationHelper()
                            .createFormulaEvaluator();
                    CellValue cellValue = evaluator.evaluate(cell);
                    return cellValue != null ? cellValue.getStringValue() : "";
                }
                default -> {
                    return "";
                }
            }
        } catch (Exception e) {
            log.warn("Could not extract cell value: {}", e.getMessage());
            return "";
        }
    }

    // Comprehensive cell value extraction
    private Object safeCellValueExtraction(Cell cell) {
        if (cell == null) return null;

        try {
            switch (cell.getCellType()) {
                case NUMERIC -> {
                    if (DateUtil.isCellDateFormatted(cell)) {
                        return cell.getDateCellValue();
                    }
                    // Handle numeric values
                    double numValue = cell.getNumericCellValue();
                    // Return as integer if whole number
                    return numValue == (long) numValue
                            ? (long) numValue
                            : numValue;
                }
                case STRING -> {
                    String stringValue = cell.getStringCellValue();
                    return (stringValue != null && !stringValue.trim().isEmpty())
                            ? stringValue.trim()
                            : null;
                }
                case BOOLEAN -> {return cell.getBooleanCellValue();}
                case FORMULA -> {
                    // Evaluate formula
                    FormulaEvaluator evaluator = cell.getSheet().getWorkbook().getCreationHelper()
                            .createFormulaEvaluator();
                    CellValue cellValue = evaluator.evaluate(cell);

                    if (cellValue == null) return null;

                    switch (cellValue.getCellType()) {
                        case NUMERIC -> {
                            double numValue = cellValue.getNumberValue();
                            return numValue == (long) numValue
                                    ? (long) numValue
                                    : numValue;
                        }
                        case STRING -> {
                            String stringValue = cellValue.getStringValue();
                            return (stringValue != null && !stringValue.trim().isEmpty())
                                    ? stringValue.trim()
                                    : null;
                        }
                        case BOOLEAN -> {
                            return cellValue.getBooleanValue();
                        }
                        default -> {
                            return null;
                        }
                    }
                }
                default -> {return null;}
            }
        } catch (Exception e) {
            log.warn("Could not process cell value: {}", e.getMessage());
            return null;
        }
    }
}
