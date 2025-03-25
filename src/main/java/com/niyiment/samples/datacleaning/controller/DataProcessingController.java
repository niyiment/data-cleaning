package com.niyiment.samples.datacleaning.controller;


import com.niyiment.samples.datacleaning.dto.CleanedDataResult;
import com.niyiment.samples.datacleaning.dto.DataQualityReport;
import com.niyiment.samples.datacleaning.service.DataProcessingService;
import jakarta.servlet.http.HttpSession;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.io.IOException;


@Controller
@RequiredArgsConstructor
public class DataProcessingController {
    private final DataProcessingService processingService;

    @GetMapping("/")
    public String uploadPage() {
        return "upload";
    }

    @PostMapping("/process")
    public String processFile(@RequestParam("file") MultipartFile file,
                              Model model, HttpSession session) {
        try{
            CleanedDataResult cleanedDataResult = processingService.processFile(file);

            model.addAttribute("cleanedData", cleanedDataResult.cleanedData());
            model.addAttribute("columns", cleanedDataResult.columns());
            session.setAttribute("cleanedDataResult", cleanedDataResult);
            if (cleanedDataResult.dataQualityReport() != null) {
                model.addAttribute("dataQualityReport", cleanedDataResult.dataQualityReport());
            } else {
                DataQualityReport dataQualityReport = DataQualityReport.builder()
                        .totalRecords(0)
                        .processedRecords(0)
                        .build();

                model.addAttribute("dataQualityReport", dataQualityReport);
            }

            return "preview";
        } catch (Exception e) {
            model.addAttribute("error","Processing file failed with message: " + e.getMessage());
            return "upload";
        }
    }

    @PostMapping("/download")
    public ResponseEntity<byte[]> downloadCleanedFile(
            @SessionAttribute("cleanedDataResult") CleanedDataResult cleanedData
    ) throws IOException {
        byte[] excelContent = processingService.exportToExcel(cleanedData);

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=cleaned_data.xlsx")
                .body(excelContent);
    }
}
