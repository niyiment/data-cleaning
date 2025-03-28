package com.niyiment.samples.datacleaning.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.niyiment.samples.datacleaning.dto.CleanedDataResult;
import com.niyiment.samples.datacleaning.dto.DataQualityReport;
import com.niyiment.samples.datacleaning.exception.ReportProcessingException;
import com.niyiment.samples.datacleaning.service.impl.DataValidationStep;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;



@Service
@Slf4j
@RequiredArgsConstructor
public class DataProcessingService {
    private final ObjectMapper objectMapper;
    private final CleaningPipeline cleaningPipeline;
    private final DataValidationStep dataValidationStep;
    private static final DateTimeFormatter[] DATE_FORMATTERS = {
            DateTimeFormatter.ISO_LOCAL_DATE, DateTimeFormatter.ofPattern("d/M/yy"),DateTimeFormatter.ofPattern("M/d/yy"),
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),DateTimeFormatter.ofPattern("MM/dd/yy"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),DateTimeFormatter.ofPattern("MM-dd-yy"),
            DateTimeFormatter.ofPattern("d/M/yy"), DateTimeFormatter.ofPattern("M/d/yy"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),DateTimeFormatter.ofPattern("dd-MM-yy"),
            DateTimeFormatter.ofPattern("d-M-yy"), DateTimeFormatter.ofPattern("M-d-yy"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd")
    };

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

            Iterator<String[]> iterator = csvReader.iterator();
            while (iterator.hasNext()) {
                String[] lines = iterator.next();
                Map<String, Object> row = new HashMap<>();
                for (int i = 0; i < cleanedHeaders.size(); i++) {
                    String value = i < lines.length ? lines[i] : "";
                    Object cellValue = value.trim().isEmpty() ? "N/A" : parseCellValue(value);
                    row.put(standardizeColumnName(cleanedHeaders.get(i)), cellValue);
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
                    .map(cell -> standardizeColumnName(Optional.ofNullable(cell).map(Cell::getStringCellValue).orElse("")))
                    .toList();

            Iterator<Row> rowIterator = sheet.iterator();
            rowIterator.next();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                if (row == null) continue;

                Map<String, Object> rowData = new HashMap<>();
                boolean hasNonEmptyValue = false;
                for (int columnNumber=0; columnNumber < headers.size(); columnNumber++) {
                    Cell cell = row.getCell(columnNumber, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    String columnName = headers.get(columnNumber);
                    Object cellValue = safeGetCellValueWithDateHandling(cell, columnName);

                    if (cellValue != null && !cellValue.toString().trim().isEmpty() && !cellValue.equals("N/A")) {
                        hasNonEmptyValue = true;
                    }
                    rowData.put(headers.get(columnNumber), cellValue == null ? "N/A" : cellValue);
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

            CellStyle headerStyle = createHeaderStyle(workbook);
            CellStyle dateStyle = createDateStyle(workbook);
            CellStyle numericStyle = createNumericStyle(workbook);
            CellStyle percentStyle = createPercentStyle(workbook);

            Row headerRow = sheet.createRow(0);
            List<String> columns = cleanedDataResult.columns();
            for (int i = 0; i < columns.size(); i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(columns.get(i));
                cell.setCellStyle(headerStyle);
            }

            List<Map<String, Object>> cleanedData = cleanedDataResult.cleanedData();
            for (int rowNum = 0; rowNum < cleanedData.size(); rowNum++) {
                Row row = sheet.createRow(rowNum + 1);
                Map<String, Object> dataRow = cleanedData.get(rowNum);

                for (int colNum = 0; colNum < columns.size(); colNum++) {
                    Cell cell = row.createCell(colNum);
                    Object value = dataRow.get(columns.get(colNum));

                    if (value != null) {
                        setCellValueWithProperType(cell, value, dateStyle, numericStyle, percentStyle);
                    }
                }
            }

            for (int i = 0; i < columns.size(); i++) {
                sheet.autoSizeColumn(i);
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            workbook.write(outputStream);
            return outputStream.toByteArray();
        }
    }

    private void setCellValueWithProperType(Cell cell, Object value,
                                            CellStyle dateStyle, CellStyle numericStyle,
                                            CellStyle percentStyle) {
        if (value instanceof String stringValue) {
            cell.setCellValue(stringValue);
        } else if (value instanceof Number numberValue) {
            if (value instanceof Integer intValue) {
                cell.setCellValue(intValue);
                cell.setCellStyle(numericStyle);
            } else if (value instanceof Long longValue) {
                cell.setCellValue(longValue);
                cell.setCellStyle(numericStyle);
            } else if (value instanceof Double doubleValue) {
                cell.setCellValue(doubleValue);
                if (doubleValue >= 0 && doubleValue <= 1) {
                    cell.setCellStyle(percentStyle);
                    cell.setCellValue(doubleValue);
                } else {
                    cell.setCellStyle(numericStyle);
                }
            } else if (value instanceof Float floatValue) {
                cell.setCellValue(floatValue);
                cell.setCellStyle(numericStyle);
            } else {
                cell.setCellValue(numberValue.toString());
            }
        } else if (value instanceof Boolean booleanValue) {
            cell.setCellValue(booleanValue);
        } else if (value instanceof LocalDate localDateValue) {
            cell.setCellValue(localDateValue);
            cell.setCellStyle(dateStyle);
        } else if (value instanceof Date dateValue) {
            cell.setCellValue(dateValue);
            cell.setCellStyle(dateStyle);
        } else if (value instanceof LocalDateTime localDateTimeValue) {
            cell.setCellValue(localDateTimeValue);
            cell.setCellStyle(dateStyle);
        } else {
            cell.setCellValue(value.toString());
        }
    }

    private CellStyle createHeaderStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        font.setFontHeightInPoints((short) 12);
        style.setFont(font);
        style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderTop(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setAlignment(HorizontalAlignment.CENTER);
        return style;
    }

    private CellStyle createDateStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        CreationHelper createHelper = workbook.getCreationHelper();
        style.setDataFormat(createHelper.createDataFormat().getFormat("yyyy-mm-dd"));
        return style;
    }

    private CellStyle createNumericStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        style.setAlignment(HorizontalAlignment.RIGHT);
        return style;
    }

    private CellStyle createPercentStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        style.setDataFormat(workbook.createDataFormat().getFormat("0.00%"));
        style.setAlignment(HorizontalAlignment.RIGHT);
        return style;
    }

    public byte[] exportValidationErrorsToCSV(List<String> validationErrors) throws IOException {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Validation Errors");
            Row headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("Error Message");

            for(int i = 0; i < validationErrors.size(); i++) {
                Row row = sheet.createRow(i+1);
                row.createCell(0).setCellValue(validationErrors.get(i));
            }

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
        List<Map<String, Object>> cleanedData = cleaningPipeline.execute(data);
        DataQualityReport report = generateDataQualityReport(data, cleanedData);
        List<String> validationErrors = dataValidationStep.getValidationResult().getErrors();

        return CleanedDataResult.builder()
                .cleanedData(cleanedData)
                .dataQualityReport(report)
                .columns(new ArrayList<>(cleanedData.get(0).keySet()))
                .validationErrors(validationErrors)
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

    private DataQualityReport generateDataQualityReport(List<Map<String, Object>> rawData,
                                                        List<Map<String, Object>> cleanedData) {
        Map<String, Long> missingValuesCount = rawData.stream()
                .flatMap(row -> row.entrySet().stream())
                .filter(entry -> entry.getValue() == null || (entry.getValue() instanceof String && entry.getValue().equals("N/A")))
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.counting()));

        Map<String, Integer> uniqueValuesCount = cleanedData.stream()
                .flatMap(row -> row.entrySet().stream())
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.collectingAndThen(
                        Collectors.mapping(Map.Entry::getValue, Collectors.toSet()),
                        Set::size)));

        Map<String, List<Double>> numericColumns = new HashMap<>();
        for (Map<String, Object> row : cleanedData) {
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (entry.getValue() instanceof Number) {
                    numericColumns.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                        .add(((Number) entry.getValue()).doubleValue());
                }
            }
        }

        Map<String, Map<String, Object>> numericStats = new HashMap<>();
        for(Map.Entry<String, List<Double>> entry : numericColumns.entrySet()) {
            List<Double> values = entry.getValue();
            if (!values.isEmpty()) {
                double sum = values.stream().mapToDouble(Double::doubleValue).sum();
                double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                double min = values.stream().mapToDouble(Double::doubleValue).min().orElse(0.0);
                double max = values.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
                numericStats.put(entry.getKey(), Map.of("mean", mean, "sum", sum, "min", min, "max", max));
            }
        }

        Map<String, String> columnTypes = new HashMap<>();
        for (Map<String, Object> row : cleanedData) {
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value != null && !value.equals("N/A")) {
                    columnTypes.computeIfAbsent(key, k -> value.getClass().getSimpleName());
                }
            }
        }

        return DataQualityReport.builder()
                .totalRecords(rawData.size())
                .processedRecords(cleanedData.size())
                .missingValuesCount(missingValuesCount)
                .uniqueValuesCount(uniqueValuesCount)
                .numericStats(numericStats)
                .columnTypes(columnTypes)
                .build();
    }

    private String getFileExtension(String filename) {
        return filename.substring(filename.lastIndexOf('.') + 1);
    }

    public static Object safeGetCellValueWithDateHandling(Cell cell, String columnName) {
        if (cell == null) return null;

        try {
            if (cell == null) {
                return null;
            }

            switch (cell.getCellType()) {
                case STRING:
                    return cell.getStringCellValue();

                case NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)) {
                        return cell.getDateCellValue()
                                .toInstant()
                                .atZone(ZoneId.systemDefault())
                                .toLocalDate();
                    } else {
                        double numericValue = cell.getNumericCellValue();
                        if (numericValue == Math.floor(numericValue)) {
                            return (long) numericValue;
                        }
                        return numericValue;
                    }

                case BOOLEAN:
                    return cell.getBooleanCellValue();

                case FORMULA:
                    return evaluateFormula(cell);

                case BLANK:
                    return null;

                default:
                    return cell.toString();
            }

        } catch (Exception e) {
            log.warn("Could not extract cell value: {}", e.getMessage());
            return null;
        }
    }

    private static Object evaluateFormula(Cell cell) {
        FormulaEvaluator evaluator = cell.getSheet()
                .getWorkbook()
                .getCreationHelper()
                .createFormulaEvaluator();

        CellValue cellValue = evaluator.evaluate(cell);
        switch (cellValue.getCellType()) {
            case NUMERIC:
                return cellValue.getNumberValue();
            case STRING:
                return cellValue.getStringValue();
            case BOOLEAN:
                return cellValue.getBooleanValue();
            default:
                return null;
        }
    }

    private Object parseCellValue(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }

        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            } else {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException ignore){}

        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                return LocalDate.parse(value, formatter);
            } catch (DateTimeParseException ignore) {}
        }

        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value);
        }

        return value;
    }
}
