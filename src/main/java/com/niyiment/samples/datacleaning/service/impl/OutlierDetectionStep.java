package com.niyiment.samples.datacleaning.service.impl;

import com.niyiment.samples.datacleaning.service.CleaningStep;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class OutlierDetectionStep implements CleaningStep {
    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> data) {
        if (data.isEmpty()) return data;

        Map<String, List<Double>> numericColumns = new HashMap<>();
        for (Map<String, Object> row : data) {
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (entry.getValue() instanceof Number) {
                    numericColumns.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                        .add(((Number) entry.getValue()).doubleValue());
                }
            }
        }

        Map<String, Double> lowerBounds = new HashMap<>();
        Map<String, Double> upperBounds = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : numericColumns.entrySet()) {
            List<Double> values = entry.getValue().stream().sorted().toList();
            int size = values.size();
            double q1 = values.get(size / 4);
            double q3 = values.get(3 * size / 4);
            double iqr = q3 - q1;
            lowerBounds.put(entry.getKey(), q1 - 1.5 * iqr);
            upperBounds.put(entry.getKey(), q3 + 1.5 * iqr);
        }

        return data.stream().map(row -> row.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    Object value = entry.getValue();
                    if (value instanceof Number) {
                        double numValue = ((Number) value).doubleValue();
                        String key = entry.getKey();
                        if (numValue < lowerBounds.getOrDefault(key, Double.NEGATIVE_INFINITY) ||
                            numValue > upperBounds.getOrDefault(key, Double.POSITIVE_INFINITY)) {
                            log.info("Outlier detected in column {}: {}", key, numValue);
                            return "OUTLIER_" + numValue;
                        }
                    }
                    return value;
                }
            ))).toList();
    }
}