package com.niyiment.samples.datacleaning.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ValidationResult {
    private boolean isValid = true;
    private List<String> errors = new ArrayList<>();

    public void addError(String error) {
        errors.add(error);
        isValid = false;
    }
}