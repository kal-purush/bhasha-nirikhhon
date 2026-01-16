package com.linepro.modellbahn.configuration;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import com.google.common.net.HttpHeaders;
import com.linepro.modellbahn.controller.impl.ApiPaths;

@Service
public class JsonRequestFilter {

    public boolean isJsonRequest(HttpServletRequest request) {
        String path = StringUtils.isBlank(request.getContextPath()) ? request.getRequestURI() : request.getContextPath();

        return path.startsWith(ApiPaths.API_ROOT) ||
               path.startsWith(ApiPaths.MANAGEMENT_ROOT) || 
               StringUtils.contains(request.getHeader(HttpHeaders.ACCEPT), MediaType.APPLICATION_JSON_VALUE) || 
               StringUtils.contains(request.getHeader(HttpHeaders.ACCEPT), ApiPaths.APPLICATION_HAL_JSON);
    }
}