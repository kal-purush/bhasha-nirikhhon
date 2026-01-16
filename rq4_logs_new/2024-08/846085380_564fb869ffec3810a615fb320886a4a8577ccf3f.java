package vn.hust.hedspi.ezsport.exceptions;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;


@Getter
public enum ErrorCode {
    UNAUTHENTICATED(1001,"Unauthenticated"),
    USER_NOT_FOUND(1002,"User not found !"),
    UNCATEGORIZED_EXCEPTION(9999, "Uncategorized error"),
    INVALID_KEY(9998,"Invalid key"),
    FIELD_NAME_INVALID(1003,"Field name invalid")
    ;

    ErrorCode(int code,String message){
        this.code = code;
        this.message = message;
    }
    private int code;
    private String message;
}