package com.niyiment.samples.datacleaning.service.impl;

import com.niyiment.samples.datacleaning.config.TypeInferenceProperties;
import com.niyiment.samples.datacleaning.service.CleaningStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class TypeInferenceStep implements CleaningStep {
    private final TypeInferenceProperties properties;

    private static final DateTimeFormatter[] DATE_FORMATTERS = {
            DateTimeFormatter.ofPattern("d/M/yyyy"), DateTimeFormatter.ofPattern("M/d/yyyy"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy"), DateTimeFormatter.ofPattern("MM/dd/yyyy"),
            DateTimeFormatter.ofPattern("yyyy-M-d"), DateTimeFormatter.ofPattern("yyyy-MM-dd"),
            DateTimeFormatter.ofPattern("d-M-yyyy"), DateTimeFormatter.ofPattern("MM-dd-yyyy"),
            DateTimeFormatter.ofPattern("d.M.yyyy"), DateTimeFormatter.ofPattern("yyyy.MM.dd"),
            DateTimeFormatter.ISO_LOCAL_DATE
    };

    private static final double EXCEL_DATE_MIN = 2.0;
    private static final double EXCEL_DATE_MAX = 109573.0;
    private static final long INTEGER_MAX_VALUE = Integer.MAX_VALUE;
    private static final long LONG_MAX_VALUE = Long.MAX_VALUE;

    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> data) {
        if (data.isEmpty()) return data;

        Map<String, Class<?>> columnTypes = inferColumnTypes(data);

        return data.stream()
                .map(row -> convertRowToInferredTypes(row, columnTypes))
                .toList();
    }

    private Map<String, Class<?>> inferColumnTypes(List<Map<String, Object>> data) {
        Map<String, Class<?>> columnTypes = new HashMap<>();

        for (Map<String, Object> row : data) {
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (isValidValue(value)) {
                    columnTypes.merge(key,
                            inferType(value, key),
                            this::mergeColumnTypes
                    );
                }
            }
        }

        return columnTypes;
    }

    private Class<?> mergeColumnTypes(Class<?> existingType, Class<?> newType) {
        if (existingType == String.class) return String.class;
        if (newType == String.class) return existingType;

        if (existingType == Double.class || newType == Double.class) return Double.class;
        if (existingType == Long.class || newType == Long.class) return Long.class;
        if (existingType == Integer.class || newType == Integer.class) return Integer.class;
        if (existingType == Boolean.class || newType == Boolean.class) return Boolean.class;

        return existingType;
    }

    private boolean isValidValue(Object value) {
        return value != null
                && !value.toString().trim().isEmpty()
                && !value.toString().equalsIgnoreCase("N/A");
    }

    private Class<?> inferType(Object value, String columnName) {
        if (value instanceof LocalDate) return LocalDate.class;
        if (value instanceof Integer) return Integer.class;
        if (value instanceof Long) return Long.class;
        if (value instanceof Double) return Double.class;
        if (value instanceof Boolean) return Boolean.class;

        String stringValue = value.toString().trim();

        if (isLikelyDateColumn(columnName)) {
            LocalDate parsedDate = tryParseDateFromString(stringValue);
            if (parsedDate != null) return LocalDate.class;
        }

        if (value instanceof Number) {
            double numValue = ((Number) value).doubleValue();
            if (numValue >= EXCEL_DATE_MIN && numValue <= EXCEL_DATE_MAX) {
                return LocalDate.class;
            }
        }

        if (isInteger(stringValue)) return Integer.class;
        if (isLong(stringValue)) return Long.class;
        if (isDouble(stringValue)) return Double.class;
        if (isBoolean(stringValue)) return Boolean.class;

        if (isIdentifierColumn(columnName)) return String.class;

        return String.class;
    }

    private Map<String, Object> convertRowToInferredTypes(
            Map<String, Object> row,
            Map<String, Class<?>> columnTypes) {
        return row.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            Object value = entry.getValue();
                            if (!isValidValue(value)) return value;

                            Class<?> targetType = columnTypes.getOrDefault(
                                    entry.getKey(),
                                    String.class
                            );
                            return convertValue(value, targetType);
                        }
                ));
    }

    private Object convertValue(Object value, Class<?> targetType) {
        if (value == null) return null;

        if (targetType.isInstance(value)) return value;

        String stringValue = value.toString().trim();

        try {
            if (targetType == Integer.class) return Integer.parseInt(stringValue);
            if (targetType == Long.class) return Long.parseLong(stringValue);
            if (targetType == Double.class) return Double.parseDouble(stringValue);
            if (targetType == Boolean.class) return Boolean.parseBoolean(stringValue);
            if (targetType == LocalDate.class) {
                return parseLocalDate(value);
            }
        } catch (Exception e) {
            log.warn("Failed to convert value '{}' to type {}", value, targetType.getSimpleName());
        }

        return stringValue;
    }

    private LocalDate parseLocalDate(Object value) {
        if (value instanceof Number) {
            return excelSerialToLocalDate(((Number) value).doubleValue());
        }

        String stringValue = value.toString().trim();

        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                return LocalDate.parse(stringValue, formatter);
            } catch (Exception ignored) {}
        }

        return null;
    }

    private boolean isLikelyDateColumn(String columnName) {
        if (columnName == null || properties.getDateColumnKeywords() == null) return false;

        return properties.getDateColumnKeywords().stream()
                .anyMatch(keyword -> columnName.toLowerCase().contains(keyword.toLowerCase()));
    }

    private LocalDate tryParseDateFromString(String stringValue) {
        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                return LocalDate.parse(stringValue, formatter);
            } catch (Exception ignored) {}
        }
        return null;
    }

    private boolean isIdentifierColumn(String columnName) {
        if (columnName == null || properties.getIdentifierPatterns() == null) return false;

        return properties.getIdentifierPatterns().stream()
                .anyMatch(pattern -> columnName.toLowerCase().matches(pattern));
    }

    private boolean isInteger(String value) {
        try {
            long longValue = Long.parseLong(value);
            return longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isLong(String value) {
        try {
            Long.parseLong(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isDouble(String value) {
        return value.matches("-?\\d*\\.\\d+");
    }

    private boolean isBoolean(String value) {
        return value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false");
    }

    private LocalDate excelSerialToLocalDate(double serial) {
        LocalDate baseDate = LocalDate.of(1899, 12, 31);
        long days = (long) serial;
        if (serial >= 60) {
            days--;
        }
        return baseDate.plusDays(days);
    }
}