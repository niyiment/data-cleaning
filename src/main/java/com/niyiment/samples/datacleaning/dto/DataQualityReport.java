package com.niyiment.samples.datacleaning.dto;


import java.util.Map;

public record DataQualityReport(
        int totalRecords, int processedRecords,
        Map<String, Long> missingValuesCount,
        Map<String, Integer> uniqueValuesCount
) {

}
