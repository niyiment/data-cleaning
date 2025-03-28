package com.niyiment.samples.datacleaning.service;

import java.util.List;
import java.util.Map;

public interface CleaningStep {
    List<Map<String, Object>> process(List<Map<String, Object>> data);
}