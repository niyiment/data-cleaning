package com.niyiment.samples.datacleaning.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.niyiment.samples.datacleaning.dto.CleanedDataResult;
import com.niyiment.samples.datacleaning.exception.ReportProcessingException;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class DataProcessingService {
    private final ObjectMapper objectMapper;

    public CleanedDataResult processFile(MultipartFile file) {
        log.debug("Initializing data cleaning process");

        String filename = file.getOriginalFilename();
        String fileExtension = getFileExtension(filename);

        List<Map<String, Object>> rawData;
        switch (fileExtension.toLowerCase()) {
            case "csv":
                rawData = processCSV(file);
                break;
            default:
                throw new IllegalArgumentException("Unsupported file format: " + fileExtension);
        }

       return toCleanedData(rawData);
    }

    public List<Map<String, Object>> processCSV(MultipartFile file) {
        log.debug("Processing CSV file");

        List<Map<String, Object>> data = new ArrayList<>();
        try (Reader reader = new InputStreamReader(file.getInputStream());
         CSVReader csvReader = new CSVReader(reader)) {
            String[] headers = csvReader.readNext();
            String[] lines;
            while ((lines = csvReader.readNext()) != null) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    row.put(headers[i], lines[i]);
                }
                data.add(row);
            }
        } catch (CsvValidationException | IOException e) {
            throw new ReportProcessingException("Error processing CSV file", e);
        }

        return data;
    }

    private String getFileExtension(String filename) {
        return filename.substring(filename.lastIndexOf('.') + 1);
    }

    private CleanedDataResult toCleanedData(List<Map<String, Object>> data) {
        return CleanedDataResult.builder()
                .cleanedData(data)
                .dataQueryReport(null)
                .columns(null)
                .build();
    }
}
