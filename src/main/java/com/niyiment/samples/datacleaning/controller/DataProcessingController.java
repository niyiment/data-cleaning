package com.niyiment.samples.datacleaning.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.niyiment.samples.datacleaning.dto.CleanedDataResult;
import com.niyiment.samples.datacleaning.dto.DataQualityReport;
import com.niyiment.samples.datacleaning.service.DataProcessingService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.io.IOException;
import java.util.List;
import java.util.Map;



@Slf4j
@Controller
@RequiredArgsConstructor
public class DataProcessingController {
    private final DataProcessingService processingService;
    private final ObjectMapper objectMapper;

    @GetMapping("/")
    public String uploadPage() {
        return "upload";
    }

    @PostMapping("/process")
    public String uploadAndProcessFile(
            @RequestParam("file") MultipartFile file,
            RedirectAttributes redirectAttributes,
            HttpSession session
    ) {
        try {
            if (file.isEmpty()) {
                redirectAttributes.addFlashAttribute("errorMessage", "Uploaded file is empty");
                return "redirect:/";
            }
            CleanedDataResult cleanedDataResult = processingService.processFile(file);

            session.setAttribute("fullResultJson", objectMapper.writeValueAsString(cleanedDataResult));
            session.setAttribute("originalFileName", file.getOriginalFilename());

            return "redirect:/results?page=0&size=10";
        } catch (Exception e) {
            log.error("Error processing file", e);
            redirectAttributes.addFlashAttribute("errorMessage",
                    "Error processing file: " + e.getMessage());
            return "redirect:/";
        }
    }

    @GetMapping("/results")
    public String viewProcessedResults(
            Model model,
            @PageableDefault(size = 10, page = 0) Pageable pageable,
            HttpSession session
    ) {
        try {
            String fullResultJsonString = (String) session.getAttribute("fullResultJson");
            String fileName = (String) session.getAttribute("originalFileName");

            if (fullResultJsonString == null || fileName == null) {
                model.addAttribute("errorMessage", "No processed data found in session");
                return "redirect:/";
            }
            CleanedDataResult cleanedDataResult = objectMapper.readValue(fullResultJsonString, CleanedDataResult.class);

            List<Map<String, Object>> cleanedData = cleanedDataResult.cleanedData();
            if (cleanedData == null || cleanedData.isEmpty()) {
                log.warn("Cleaned data is empty or null");
                model.addAttribute("errorMessage", "No data available to display");
                return "preview";
            }

            int start = (int) pageable.getOffset();
            int end = Math.min((start + pageable.getPageSize()), cleanedData.size());
            List<Map<String, Object>> pageContent = start < cleanedData.size()
                    ? cleanedData.subList(start, end)
                    : List.of();
            Page<Map<String, Object>> pagedResults = new PageImpl<>(
                    pageContent, pageable, cleanedData.size()
            );

            model.addAttribute("result", cleanedDataResult);
            model.addAttribute("cleanedData", cleanedDataResult.cleanedData());
            model.addAttribute("columns", cleanedDataResult.columns());
            model.addAttribute("pagedData", pagedResults);
            model.addAttribute("validationErrors", cleanedDataResult.validationErrors());

            // Handle Data Quality Report
            DataQualityReport dataQualityReport = cleanedDataResult.dataQualityReport() != null
                    ? cleanedDataResult.dataQualityReport()
                    : DataQualityReport.builder()
                    .totalRecords(0)
                    .processedRecords(0)
                    .build();

            model.addAttribute("dataQualityReport", dataQualityReport);

            return "preview";
        } catch (Exception e) {
            log.error("Error retrieving processed results", e);
            model.addAttribute("errorMessage", "Error retrieving results: " + e.getMessage());
            return "redirect:/";
        }
    }

    @GetMapping("/download")
    public ResponseEntity<byte[]> downloadCleanedFile(HttpSession session) throws IOException {
        String fullResultJsonString = (String) session.getAttribute("fullResultJson");
        String fileName2 = (String) session.getAttribute("originalFileName");
        CleanedDataResult cleanedDataResult = objectMapper.readValue(
                fullResultJsonString,
                CleanedDataResult.class
        );
        byte[] excelContent = processingService.exportToExcel(cleanedDataResult);

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=" + fileName2.replace(".", "_cleaned."))
                .body(excelContent);
    }

    @GetMapping("/download-error-log")
    public ResponseEntity<byte[]> downloadErrorLog(HttpSession session) throws Exception {
        String fullResultJsonString = (String) session.getAttribute("fullResultJson");
        if (fullResultJsonString == null) {
            return ResponseEntity.badRequest().body("No processed data found in session".getBytes());
        }

        CleanedDataResult cleanedDataResult = objectMapper.readValue(fullResultJsonString, CleanedDataResult.class);
        List<String> validationErrors = cleanedDataResult.validationErrors();

        if (validationErrors == null || validationErrors.isEmpty()) {
            return ResponseEntity.badRequest().body("No validation errors to download".getBytes());
        }

        byte[] errorLog = processingService.exportValidationErrorsToCSV(validationErrors);

        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"validation_errors.xlsx\"")
                .body(errorLog);
    }
}


