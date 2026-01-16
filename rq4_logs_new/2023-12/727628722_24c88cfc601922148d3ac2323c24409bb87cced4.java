package com.spartanullnull.otil.global.exception;

public enum ErrorCase {
    /* Common */
    UNKNOWN("unKnown"),
    COMMUNICATION_ERROR("communicationError"),
    DATA_ERROR("dataError"),

    /* User */
    USER_NOT_FOUND("userNotFound"),
    EMAIL_SIGNIN_FAILED("emailSigninFailed"),
    ENTRY_POINT_EXCEPTION("entryPointException"),
    ACCESS_DENIED("accessDenied"),
    EXISTING_USER("existingUser"),
    REQUIRES_LOGIN("requiresLogin"),
    REQUIRES_LOGGED_OUT("requiresLoggedOut"),
    ACCOUNT_ID_VALIDATION_FAIL("accountIdValidationFail"),
    DUPLICATED_ACCOUNT_ID("duplicatedAccountId"),
    PASSWORD_VALIDATION_FAIL("passwordValidationFail"),
    NICKNAME_VALIDATION_FAIL("nickNameValidationFail"),
    EMAIL_VALIDATION_FAIL("emailValidationFail"),
    UNVALID_ADMIN_KEY("unvalidAdminKey"),

    /* Post */
    NOT_FOUND_POST("notFoundPost"),
    DUPLICATED_POST("duplicatedPost"),
    NOT_AUTHOR_OF_POST("notAuthorOfPost"),

    /* Comment */
    NOT_FOUND_COMMENT("notFoundComment"),
    DUPLICATED_COMMENT("duplicatedComment"),
    NOT_AUTHOR_OF_COMMENT("notAuthorOfComment"),
    INAPPROPRIATE_COMMENT("inappropriateComment"),

    /* Category */
    NOT_FOUND_CATEGORY("notFoundCategory"),
    DUPLICATED_CATEGORY("duplicatedCategory"),
    NOT_AUTHOR_OF_CATEGORY("notAuthorOfCategory"),
    INAPPROPRIATE_CATEGORY("inappropriateCategory");

    private final String key;

    ErrorCase(String key) {
        this.key = key;
    }

    public String getCode() {
        return this.key + ".code";
    }

    public String getMsg() {
        return this.key + ".msg";
    }

    public String getStatus() {
        return this.key + ".status";
    }
}