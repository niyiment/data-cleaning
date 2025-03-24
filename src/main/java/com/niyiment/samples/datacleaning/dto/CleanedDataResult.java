package com.niyiment.samples.datacleaning.dto;


import java.util.List;
import java.util.Map;

public record CleanedDataResult(
        List<Map<String, Object>> cleanedData,
        DataQualityReport dataQueryReport,
        List<String> columns
) {
}
