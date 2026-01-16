package com.spartanullnull.otil.domain.category.exception;

import com.spartanullnull.otil.domain.common.exception.*;
import com.spartanullnull.otil.global.exception.*;
import java.time.*;

public class DuplicatedCategoryException extends DomainException {

    public DuplicatedCategoryException(String field, String value, String reason) {
        super(
            ErrorCode.of(ErrorCase.DUPLICATED_CATEGORY),
            new ErrorDetail(field, value, reason, LocalDateTime.now()
            )
        );
    }

    public DuplicatedCategoryException(String field, String value) {
        super(
            ErrorCode.of(ErrorCase.DUPLICATED_CATEGORY),
            new ErrorDetail(field, value, LocalDateTime.now()
            )
        );
    }
}