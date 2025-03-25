package com.niyiment.samples.datacleaning.dto;


import lombok.Builder;

import java.util.List;
import java.util.Map;

@Builder
public record CleanedDataResult(
        List<Map<String, Object>> cleanedData,
        DataQualityReport dataQueryReport,
        List<String> columns
) {
}
