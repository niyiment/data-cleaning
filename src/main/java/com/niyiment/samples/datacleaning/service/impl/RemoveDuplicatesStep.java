package com.niyiment.samples.datacleaning.service.impl;

import com.niyiment.samples.datacleaning.service.CleaningStep;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class RemoveDuplicatesStep implements CleaningStep {
    private final List<String> keyColumns;

    public RemoveDuplicatesStep() {
        this.keyColumns = List.of();
    }

    public RemoveDuplicatesStep(List<String> keyColumns) {
        this.keyColumns = keyColumns != null ? keyColumns : List.of();
    }


    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> data) {
        if (keyColumns.isEmpty()) {
            return data.stream()
                    .distinct()
                    .collect(Collectors.toList());

        }
        Map<List<Object>, Map<String, Object>> uniqueRows = new LinkedHashMap<>();
        for (Map<String, Object> row : data) {
            List<Object> keyValues = keyColumns.stream()
                    .map(row::get)
                    .collect(Collectors.toList());
            uniqueRows.computeIfAbsent(keyValues, k -> row);
        }

        return new ArrayList<>(uniqueRows.values());
    }
}
