package soma.edupiuser.web.models;

import java.util.Collections;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class SuccessResponse {

    private static final String DEFAULT_CODE = "CM-200000";
    private static final String DEFAULT_DETAIL = "";

    private String code;
    private String detail;
    private Object result;

    @Builder
    public SuccessResponse(String code, String detail, Object result) {
        this.code = (code != null) ? code : DEFAULT_CODE;
        this.detail = (detail != null) ? detail : DEFAULT_DETAIL;
        this.result = (result != null) ? result : Collections.EMPTY_MAP;
    }

    public static SuccessResponse withDetailAndResult(String detail, Object result) {
        return SuccessResponse.builder()
            .detail(detail)
            .result(result)
            .build();
    }

    public static SuccessResponse withDetail(String detail) {
        return SuccessResponse.builder()
            .detail(detail)
            .build();
    }
}