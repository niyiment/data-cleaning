package com.niyiment.samples.datacleaning.config;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.multipart.MultipartResolver;
import org.springframework.web.multipart.support.StandardServletMultipartResolver;

@Configuration
public class ApplicationConfiguration {

    @Bean
    public MultipartResolver multipartResolver() {
        return new StandardServletMultipartResolver();
    }
}
