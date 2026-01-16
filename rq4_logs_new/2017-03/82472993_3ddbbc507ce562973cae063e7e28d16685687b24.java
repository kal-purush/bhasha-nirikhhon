package com.kvteam.backend.controllers;

import com.kvteam.backend.dataformats.ResponseStatusData;
import com.kvteam.backend.dataformats.UserData;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

/**
 * Created by maxim on 12.03.17.
 */
@SpringBootTest(webEnvironment = RANDOM_PORT)
@RunWith(SpringRunner.class)
public class BackendControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;

    private ResponseEntity<ResponseStatusData> register(String username, String passwd, HttpStatus expectedStatus){
        final ResponseEntity<ResponseStatusData> answer = restTemplate.postForEntity(
                "/api/account",
                new UserData(
                        username,
                        passwd,
                        "old@mail.ru",
                        0,
                        1
                ),
                ResponseStatusData.class
        );
        assertEquals(expectedStatus, answer.getStatusCode());
        return answer;
    }

    @Nullable
    private UserData get(String username, HttpStatus expectedStatus, @Nullable String expectedEmail){
        final ResponseEntity<LinkedHashMap> user = restTemplate.getForEntity(
                "/api/account/"+username,
                LinkedHashMap.class
        );
        assertEquals(expectedStatus, user.getStatusCode());
        if(expectedStatus == HttpStatus.OK){
            assertEquals(
                    username,
                    (user.getBody()).get("username"));
            assertEquals(
                    expectedEmail,
                    (user.getBody()).get("email"));
            return new UserData(
                    (user.getBody()).get("username").toString(),
                    null,
                    (user.getBody()).get("email").toString(),
                    null,
                    null);
        }
        return null;
    }

    private void edit(
            List<String> cookie,
            @Nullable String email,
            @Nullable String password,
            HttpStatus expectedStatus){
        final HttpHeaders requestHeaders = new HttpHeaders();
        requestHeaders.put(HttpHeaders.COOKIE, cookie);
        final HttpEntity<UserData> requestEntity = new HttpEntity<>(
                new UserData(
                        null,
                        password,
                        email,
                        null,
                        null
                ),
                requestHeaders);
        final ResponseEntity<ResponseStatusData> answer =
                restTemplate.exchange("/api/account", HttpMethod.PUT, requestEntity, ResponseStatusData.class);
        assertEquals(expectedStatus, answer.getStatusCode());
    }

    private List<String> cookieFromEntity(ResponseEntity entity){
        return entity.getHeaders().get("Set-Cookie");
    }

    private ResponseEntity<ResponseStatusData> login(String username, String password, HttpStatus expectedStatus){
        final ResponseEntity<ResponseStatusData> answerLogin = restTemplate.postForEntity(
                "/api/login",
                new UserData(
                        username,
                        password,
                        null,
                        null,
                        null
                ),
                ResponseStatusData.class
        );

        assertEquals(expectedStatus, answerLogin.getStatusCode());
        if(answerLogin.getStatusCode() == HttpStatus.OK) {
            assertEquals(username, answerLogin.getBody().getMessage());
        }
        return answerLogin;
    }

    private void logout(List<String> cookie){
        final HttpHeaders requestHeaders = new HttpHeaders();
        requestHeaders.put(HttpHeaders.COOKIE, cookie);
        final HttpEntity requestEntity = new HttpEntity(requestHeaders);
        restTemplate.exchange("/api/logout", HttpMethod.GET, requestEntity, ResponseStatusData.class);
    }

    private boolean isLoggedIn(List<String> cookie){
        final HttpHeaders requestHeaders = new HttpHeaders();
        requestHeaders.put(HttpHeaders.COOKIE, cookie);
        final HttpEntity requestEntity = new HttpEntity(requestHeaders);
        final ResponseEntity<ResponseStatusData> answer =
            restTemplate.exchange("/api/isloggedin", HttpMethod.GET, requestEntity, ResponseStatusData.class);

        return answer.getStatusCodeValue() == HttpStatus.OK.value();
    }

    @Test
    public void registerCorrect() {
        final ResponseEntity<ResponseStatusData> answer = register("user1", "123", HttpStatus.OK);
        assertEquals("user1", answer.getBody().getMessage());
    }

    @Test
    public void registerConflict() {
        final ResponseEntity<ResponseStatusData> answer1 = register("user2","123", HttpStatus.OK);
        assertEquals("user2", answer1.getBody().getMessage());

        register("user2","312", HttpStatus.CONFLICT);
    }

    @Test
    public void registerEmptyPassword() {
        register("user1","", HttpStatus.BAD_REQUEST);
    }

    @Test
    public void registerEmptyUsername() {
        register("","passwd", HttpStatus.BAD_REQUEST);
    }

    @Test
    public void getNotFound() {
        get("not_exists", HttpStatus.NOT_FOUND, null);
    }

    @Test
    public void getCorrect() {
        register("user3", "passwd", HttpStatus.OK);
        get("user3", HttpStatus.OK, "old@mail.ru");
    }


    @Test
    public void loginCorrect(){
        final ResponseEntity<ResponseStatusData> answer = register("user4", "123", HttpStatus.OK);
        logout(cookieFromEntity(answer));
        login("user4", "123", HttpStatus.OK);
    }


    @Test
    public void loginAccesDenied(){
        login("user_not_exists", "321", HttpStatus.FORBIDDEN);
        final ResponseEntity<ResponseStatusData> answer = register("user5", "123", HttpStatus.OK);
        logout(cookieFromEntity(answer));
        login("user5", "321", HttpStatus.FORBIDDEN);
    }


    @Test
    public void isLoggedIn() {
        assertFalse(isLoggedIn(new ArrayList<>()));
        final ResponseEntity answer = register("user6", "password", HttpStatus.OK);
        assertTrue(isLoggedIn(cookieFromEntity(answer)));
        logout(cookieFromEntity(answer));
        assertFalse(isLoggedIn(cookieFromEntity(answer)));
        final ResponseEntity answer2 = login("user6", "password", HttpStatus.OK);
        assertTrue(isLoggedIn(cookieFromEntity(answer2)));
        logout(cookieFromEntity(answer2));
        assertFalse(isLoggedIn(new ArrayList<>()));
    }

    @Test
    public void editCorrect(){
        final ResponseEntity<ResponseStatusData> answer = register("user7", "passwd", HttpStatus.OK);
        get("user7", HttpStatus.OK, "old@mail.ru");
        edit(cookieFromEntity(answer), "new@mail.ru", "newpasswd", HttpStatus.OK);
        get("user7", HttpStatus.OK, "new@mail.ru");
        logout(cookieFromEntity(answer));
        assertFalse(isLoggedIn(cookieFromEntity(answer)));
        final ResponseEntity answer2 = login("user7", "newpasswd", HttpStatus.OK);
        assertTrue(isLoggedIn(cookieFromEntity(answer2)));
    }

    @Test
    public void editAccessDenied(){
        final ResponseEntity<ResponseStatusData> answer = register("user8", "passwd", HttpStatus.OK);
        logout(cookieFromEntity(answer));
        edit(cookieFromEntity(answer), "new@mail.ru", "newpasswd", HttpStatus.FORBIDDEN);
    }
}