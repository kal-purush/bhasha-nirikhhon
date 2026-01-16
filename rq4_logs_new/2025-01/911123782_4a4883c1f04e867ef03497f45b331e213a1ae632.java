package com.roomfit.be.survey.application.dto;

import com.roomfit.be.survey.domain.Option;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class OptionDTO {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Request {
        private String label;
        private String value;
        public Option toEntity(){
            return new Option(label, value);
        }
    }
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Response {
        private Long id;
        private String label;
        private String value;
        public static Response of(Option option){
            return Response.builder()
                    .id(option.getId())
                    .label(option.getLabel())
                    .value(option.getValue())
                    .build();
        }

    }
}