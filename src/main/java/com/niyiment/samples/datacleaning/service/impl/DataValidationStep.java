package com.niyiment.samples.datacleaning.service.impl;

import com.niyiment.samples.datacleaning.dto.ValidationResult;
import com.niyiment.samples.datacleaning.service.CleaningStep;
import lombok.Getter;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

@Service
public class DataValidationStep implements CleaningStep {
    private final LocalDate currentDate = LocalDate.of(2025, 3, 25);
    private static final DateTimeFormatter[] DATE_FORMATTERS = {
        DateTimeFormatter.ofPattern("M/d/yyyy"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd"),
        DateTimeFormatter.ofPattern("dd-MM-yyyy")
    };

    @Getter
    private ValidationResult validationResult;

    public DataValidationStep() {
        this.validationResult = new ValidationResult();
    }

    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> data) {
        validationResult = new ValidationResult();

        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> row = data.get(i);
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (value instanceof Number) {
                    double numValue = ((Number) value).doubleValue();
                    if (key.toLowerCase().contains("age") && (numValue < 0 || numValue > 120)) {
                        validationResult.addError(String.format("Row %d, column %s: Age %s is out of range (0-120)", i, key, numValue));
                    }
                }

                if (value instanceof LocalDate) {
                    LocalDate date = (LocalDate) value;
                    if (date.isAfter(currentDate)) {
                        validationResult.addError(String.format("Row %d, column %s: Date %s is in the future", i, key, date));
                    }
                } else if (value instanceof String && !value.equals("N/A")) {
                    for (DateTimeFormatter formatter : DATE_FORMATTERS) {
                        try {
                            LocalDate date = LocalDate.parse((String) value, formatter);
                            if (date.isAfter(currentDate)) {
                                validationResult.addError(String.format("Row %d, column %s: Date %s is in the future", i, key, date));
                            }
                            break;
                        } catch (Exception ignored) {}
                    }
                }
            }
        }
        return data;
    }

}