package com.niyiment.samples.datacleaning.service.impl;

import com.niyiment.samples.datacleaning.service.CleaningStep;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class RemoveSpecialCharactersStep implements CleaningStep {

    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> data) {
        return data.stream().map(row -> row.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    Object value = entry.getValue();
                    if (value instanceof String && !value.equals("N/A")) {
                        return ((String) value).replaceAll("[^a-zA-Z0-9\\s]", "");
                    }
                    return value;
                }
            )))
            .toList();
    }
}