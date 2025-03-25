package com.niyiment.samples.datacleaning.controller;


import com.niyiment.samples.datacleaning.dto.CleanedDataResult;
import com.niyiment.samples.datacleaning.service.DataProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;


@RestController
@RequestMapping("/api/data-processing")
@RequiredArgsConstructor
public class DataProcessingController {
    private final DataProcessingService processingService;

    @PostMapping
    public ResponseEntity<CleanedDataResult> processFile(@RequestParam("file") MultipartFile file) {
        CleanedDataResult cleanupResult = processingService.processFile(file);

        return ResponseEntity.ok(cleanupResult);
    }
}
