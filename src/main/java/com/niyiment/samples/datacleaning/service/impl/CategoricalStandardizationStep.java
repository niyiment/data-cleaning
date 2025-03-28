package com.niyiment.samples.datacleaning.service.impl;


import com.niyiment.samples.datacleaning.service.CleaningStep;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class CategoricalStandardizationStep implements CleaningStep {
    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> data) {
        if (data.isEmpty()) return data;

        Map<String, Set<Object>> uniqueValues = new HashMap<>();
        for (Map<String, Object> row : data) {
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                uniqueValues.computeIfAbsent(entry.getKey(), k -> new HashSet<>())
                    .add(entry.getValue());
            }
        }

        List<String> categoricalColumns = uniqueValues.entrySet().stream()
            .filter(e -> e.getValue().size() <= data.size() / 2)
            .map(Map.Entry::getKey)
            .toList();

        return data.stream().map(row -> row.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    if (categoricalColumns.contains(entry.getKey()) && entry.getValue() instanceof String) {
                        String value = (String) entry.getValue();
                        return value.trim().replaceAll("\\s+", "_");
                    }
                    return entry.getValue();
                }
            )))
            .toList();
    }
}