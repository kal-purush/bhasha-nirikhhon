package com.prime.models.request;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Builder
@Data
public class CommonPageInfo<T> {

    public Integer page;

    public Integer size;

    public Long total;

    public List<T> data;

    public String __typename;

    public static final class Fields {
        public static final String page = "page";
        public static final String size = "size";
        public static final String total = "total";
        public static final String data = "data";
        public static final String __typename = "__typename";
    }
}