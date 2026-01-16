package com.devpuccino.accountservice.filter;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Order(2)
public class LoggingFilter extends OncePerRequestFilter {
    @Autowired
    private ObjectMapper objectMapper;

    private static final Logger logger = LogManager.getLogger(LoggingFilter.class);
    private static final List<String> allowHeaders = Arrays.asList("correlationId","content-type","authorization");

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws ServletException, IOException {
        Long startTime = System.currentTimeMillis();
        ContentCachingResponseWrapper responseWrapper = new ContentCachingResponseWrapper(response);
        if(request instanceof HttpServletRequest){
            LoggingRequestWrapper loggingRequest = new LoggingRequestWrapper(request);
            this.logRequest(loggingRequest);
            chain.doFilter(loggingRequest,responseWrapper);

        }else{
            chain.doFilter(request,responseWrapper);
        }
        this.logResponse(responseWrapper,startTime);
    }

    private void logRequest(LoggingRequestWrapper request) throws IOException {
        StringBuilder logFormat = new StringBuilder("REQUEST [{}] url=[{}] headers=[{}] {}");
        StringBuilder parameterString = new StringBuilder();


        Map<String, String> headers = new HashMap<>();
        request.getHeaderNames().asIterator().forEachRemaining(headerName -> {
            if(allowHeaders.contains(headerName)) {
                headers.put(headerName, request.getHeader(headerName));
            }
        });

        Map<String, String> params = new HashMap<>();
        request.getParameterMap().keySet().stream().forEach(key -> {
            params.put(key, request.getParameter(key));
        });
        if (!params.isEmpty()) {
            parameterString.append(" parameter=[");
            parameterString.append(objectMapper.writeValueAsString(params));
            parameterString.append("]");
        }

        if(!request.getBody().isEmpty()){
            logFormat.append(" body=[{}]");
            Map<String,Object> bodyObject = objectMapper.readValue(request.getBody(), new TypeReference<>() {
            });
            logger.info(logFormat.toString(),
                    request.getMethod(),
                    request.getRequestURI(),
                    objectMapper.writeValueAsString(headers),
                    parameterString,
                    objectMapper.writeValueAsString(bodyObject)
                    );
        }else{
            logger.info(logFormat.toString(),
                    request.getMethod(), request.getRequestURI(),
                    objectMapper.writeValueAsString(headers),
                    parameterString
            );
        }

    }
    private void logResponse(ContentCachingResponseWrapper response,Long startTime) throws IOException {
        Long processingTime = System.currentTimeMillis() - startTime;
        logger.info("RESPONSE [{} ms] status=[{}] body={}",processingTime,response.getStatus(),new String(response.getContentAsByteArray()));
        response.copyBodyToResponse();
    }
}
class LoggingRequestWrapper extends HttpServletRequestWrapper {

    private final String body;

    public LoggingRequestWrapper(HttpServletRequest request) throws IOException {
        super(request);
        BufferedReader bufferedReader = null;
        try {
            ServletInputStream inputStream = request.getInputStream();
            if (inputStream != null) {
                StringBuilder cacheRequestBody = new StringBuilder();
                bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                String requestBodyLine;
                while ((requestBodyLine = bufferedReader.readLine()) != null) {
                    cacheRequestBody.append(requestBodyLine);
                }
                this.body = cacheRequestBody.toString();
            }else{
                this.body = "";
            }
        } catch (IOException ex) {
            throw ex;
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }
    }

    @Override
    public ServletInputStream getInputStream() {
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(this.body.getBytes());
        return new ServletInputStream() {
            @Override
            public boolean isFinished() {
                return false;
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setReadListener(ReadListener listener) {

            }

            @Override
            public int read() {
                return byteArrayInputStream.read();
            }
        };
    }

    @Override
    public BufferedReader getReader() {
        return new BufferedReader(new InputStreamReader(this.getInputStream()));
    }
    public String getBody(){
        return this.body;
    }
}