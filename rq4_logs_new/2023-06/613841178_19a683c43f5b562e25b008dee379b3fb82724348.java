package kr.pe.afterschool.domain.apply.exception;

import kr.pe.afterschool.domain.apply.error.ApplyExceptionCode;
import kr.pe.afterschool.global.error.AfterSchoolException;

public class AlreadyApplyException extends AfterSchoolException {

    private AlreadyApplyException() {
        super(ApplyExceptionCode.ALREADY_APPLY);
    }

    public static final AfterSchoolException EXCEPTION = new AlreadyApplyException();
}