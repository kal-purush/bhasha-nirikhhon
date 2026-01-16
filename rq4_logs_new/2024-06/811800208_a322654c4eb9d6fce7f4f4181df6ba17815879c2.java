package io.bootify.my_app.dto;

import lombok.Data;

import java.util.List;
@Data
public class ResponseLeaseDto {

    private String message;
    private List<LeaseDto> list;
    private String exception;
    private int totalPages;
    public ResponseLeaseDto(String message){
        this.message=message;
    }

    public ResponseLeaseDto(String message, List<LeaseDto> list, int totalPages) {
        this.message =message;
        this.list = list;
        this.totalPages = totalPages;
    }
}

