package com.apptec.camello.loginactivity.forgotpassword;

import com.apptec.camello.repository.webservices.pojoresponse.GeneralResponse;
import com.apptec.camello.util.Constants;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

public interface RecoverPassword {

    /**
     * Method to allow the user recover the password
     *
     * @param recoverPasswordBody just a body with the email of the user
     * @return GeneralResponse
     */
    @POST(Constants.RECOVER_PASSWORD_URL)
    Call<GeneralResponse> recoverPassword(@Body RecoverPasswordBody recoverPasswordBody);
}