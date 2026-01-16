package hello.hellospring2.controller.DTO;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class DalleRequest {
    private String model;
    private String prompt;
    private Long n;
    private String size;

    @Data
    public static class data {
        private List<String> images;
    }
}