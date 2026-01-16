package com.helloit.householdtracker.ux.common;

import com.sun.istack.internal.NotNull;

/**
 * Created by Student on 5/30/2016.
 */
public interface IRegisterService {


    CreationOutcomes registerAccount(@NotNull String userName, @NotNull String password, @NotNull String retypedPassword);

    enum CreationOutcomes {
        SUCCESS,
        RETYPED_PASSWORD_DO_NOT_MATCH,
        MISSING_USERNAME,
        MISSING_PASSWORD,
        EXISTING_ACCOUNT
    }

}