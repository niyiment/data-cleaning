package com.niyiment.samples.datacleaning.service;

import com.niyiment.samples.datacleaning.config.CleaningPipelineProperties;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class CleaningPipeline {
    private final CleaningPipelineProperties properties;
    private final Map<String, CleaningStep> availableSteps;
    private  List<CleaningStep> steps;

    public CleaningPipeline() {
        this.properties = null;
        this.availableSteps = null;
        this.steps = new ArrayList<>();
    }

    @Autowired
    public CleaningPipeline(CleaningPipelineProperties properties, List<CleaningStep> allSteps) {
        this.properties = properties;
        this.availableSteps = allSteps.stream()
                .collect(Collectors.toMap(step -> step.getClass().getSimpleName().replace("Step", "").toLowerCase(),
                        Function.identity()));

    }

    @PostConstruct
    public void init() {
        if (properties.getSteps() == null || properties.getSteps().isEmpty()) {
            log.warn("No cleaning steps configured in application.yml. Pipeline properties");
            this.steps = new ArrayList<>();
            return;
        }
        this.steps = new ArrayList<>();
        for (String stepName : properties.getSteps()) {
            CleaningStep step = availableSteps.get(stepName.toLowerCase());
            if (step != null) {
                this.steps.add(step);
                log.info("Added cleaning step: {}", stepName);
            } else {
                log.warn("Skipping unknown cleaning step: {}. Skipping.", stepName);
            }
        }

        if (this.steps.isEmpty()) {
            log.warn("No cleaning steps found in the pipeline. Skipping data cleaning.");
        }
    }


    public List<Map<String, Object>> execute(List<Map<String, Object>> data) {
        List<Map<String, Object>> result = data;
        for (CleaningStep step : steps) {
            log.debug("Executing cleaning step: {}", step.getClass().getSimpleName());
            result = step.process(result);
        }
        return result;
    }
}