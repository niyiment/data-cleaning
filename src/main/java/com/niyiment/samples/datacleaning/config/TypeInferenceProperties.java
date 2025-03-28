package com.niyiment.samples.datacleaning.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Data
@Component
@ConfigurationProperties(prefix = "cleaning.type-inference")
public class TypeInferenceProperties {
    private List<String> identifierPatterns;
    private List<String> dateColumnKeywords;
}