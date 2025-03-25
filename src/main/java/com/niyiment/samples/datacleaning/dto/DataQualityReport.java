package com.niyiment.samples.datacleaning.dto;


import lombok.Builder;

import java.util.Map;


@Builder
public record DataQualityReport(
        Integer totalRecords, Integer processedRecords,
        Map<String, Long> missingValuesCount,
        Map<String, Integer> uniqueValuesCount
) {

}
