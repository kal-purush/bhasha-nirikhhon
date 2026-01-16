package starter.koskita;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import net.serenitybdd.rest.SerenityRest;
import net.thucydides.core.annotations.Step;
import net.thucydides.core.annotations.Steps;
import starter.utils.Constants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class KoskitaUserAPI {

    public static String POST_CREATE_USER = Constants.BASE_URL+"/users";
    public static String DELETE_USER = Constants.BASE_URL+"/users";


    @Step("Post Create User")
    public void postCreateUser(File json){
        SerenityRest.given()
                .contentType(ContentType.JSON)
                .body(json);
    }
    @Step("Get User Profile")
    public void getUserProfile(){
        SerenityRest.given()
                .header("Authorization", "Bearer "+ Constants.CONSTANT_TOKEN_GET_USER_PROFILE);
    }
    @Step("Update User Profile")
    public void updateUserProfile(String name, String user_name, String email, String password, File photo_profile, String gender) throws IOException {
        String token = "Bearer " + Constants.getAuthToken();
        Response response = SerenityRest.given()
                .contentType("multipart/form-data")
                .header("Authorization", token)
                .multiPart("photo_profile", photo_profile.getName(), new FileInputStream(photo_profile))
                .multiPart("name", name)
                .multiPart("user_name", user_name)
                .multiPart("email", email)
                .multiPart("password", password)
                .multiPart("gender", gender)
                .put(POST_CREATE_USER);
    }

    @Step("Update User Profile gender")
    public void updateUserProfileGender(String gender) throws IOException {
        String token = "Bearer " + Constants.getAuthToken();
        Response response = SerenityRest.given()
                .contentType("multipart/form-data")
                .header("Authorization", token)
                .multiPart("gender", gender)
                .put(POST_CREATE_USER);
    }
    @Step("Update User Profile email")
    public void updateUserProfileEmail(String email) throws IOException {
        String token = "Bearer " + Constants.getAuthToken();
        Response response = SerenityRest.given()
                .contentType("multipart/form-data")
                .header("Authorization", token)
                .multiPart("email", email)
                .put(POST_CREATE_USER);
    }
    @Step("Update User Profile password")
    public void updateUserProfilePassword(String password) throws IOException {
        String token = "Bearer " + Constants.getAuthToken();
        Response response = SerenityRest.given()
                .contentType("multipart/form-data")
                .header("Authorization", token)
                .multiPart("password", password)
                .put(POST_CREATE_USER);
    }

    @Step("Delete user with valid id")
    public void deleteValidUser(){
        SerenityRest.given()
                .header("Authorization", "Bearer "+ Constants.CONSTANT_TOKEN);
    }

}

