package com.niyiment.samples.datacleaning.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.Optional;

@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(ReportProcessingException.class)
    public String handleReportProcessingException(ReportProcessingException e, Model model) {
        log.error("File processing error", e);
        model.addAttribute("errorMessage", 
            "Error processing file: " + Optional.ofNullable(e.getMessage())
                .orElse("Unknown processing error"));
        return "upload";
    }

    @ExceptionHandler(Exception.class)
    public String handleGeneralException(Exception e, Model model) {
        log.error("Unexpected error", e);
        model.addAttribute("errorMessage", 
            "An unexpected error occurred: " + Optional.ofNullable(e.getMessage())
                .orElse("Please try again"));
        return "upload";
    }
}