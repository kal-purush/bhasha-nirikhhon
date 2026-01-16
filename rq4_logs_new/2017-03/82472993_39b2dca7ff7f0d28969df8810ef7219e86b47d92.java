package com.kvteam.backend.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kvteam.backend.dataformats.ResponseStatusData;
import com.kvteam.backend.dataformats.UserData;
import com.kvteam.backend.exceptions.AccessDeniedException;
import com.kvteam.backend.services.AccountService;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.*;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Created by maxim on 12.03.17.
 */

@SuppressWarnings("OverlyBroadThrowsClause")
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
public class BackendControllerMockTest {
    @Mock
    AccountService accountService;

    @InjectMocks
    BackendController backendController;

    private MockMvc mockMvc;
    private ObjectMapper mapper;
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.mockMvc = MockMvcBuilders.standaloneSetup(backendController).build();
        this.mapper = new ObjectMapper();
    }


    private static String getUniqueUsername(){
        return "user-(" + UUID.randomUUID().toString() + ')';
    }



    private MvcResult register(
            String username,
            String passwd,
            ResultMatcher matcher,
            boolean correctRegister,
            boolean correctLogin)
                throws Exception{
        final UserData userData = new UserData(
                username,
                passwd,
                username + "@mail.ru",
                0,
                1
        );
        when(
                accountService.add(anyObject())
        ).thenReturn(correctRegister);
        when(
                accountService.login(
                        eq(userData.getUsername()),
                        eq(userData.getPassword()))
        ).thenReturn(correctLogin ? UUID.randomUUID() : null);


        return mockMvc
                .perform(
                        post("/api/account")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(mapper.writeValueAsString(userData))
                )
                .andExpect(matcher)
                .andReturn();
    }


    private void getAcc(String username, @Nullable String expectedEmail, ResultMatcher matcher )
                    throws Exception{
        when(
                accountService.get(eq(username))
        ).thenReturn(expectedEmail == null ? null : new UserData(username, expectedEmail,0,1));
        final MvcResult result = mockMvc
                .perform(
                        get("/api/account/" + username)
                )
                .andExpect(matcher)
                .andReturn();
        if(expectedEmail != null){
            final UserData answer =
                    mapper.readValue(result.getResponse().getContentAsString(), UserData.class);
            assertEquals(username, answer.getUsername());
            assertEquals(expectedEmail, answer.getEmail());
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void edit(
            MockHttpSession session,
            @Nullable String email,
            @Nullable String password,
            ResultMatcher matcher,
            boolean correct) throws Exception{
        final UserData userData = new UserData(
                null,
                password,
                email,
                null,
                null
        );
        final String username = session.getAttribute("username") != null ?
                session.getAttribute("username").toString() :
                "";
        final UUID sessionID = session.getAttribute("sessionID") != null ?
                (UUID)session.getAttribute("sessionID") :
                null;
        if(!correct){
            doThrow(new AccessDeniedException())
                    .when(accountService)
                    .editAccount(
                        eq(username),
                        eq(sessionID),
                        eq(userData.getEmail()),
                        eq(userData.getPassword()));

        }

        mockMvc
                .perform(
                        put("/api/account")
                                .session(session)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(mapper.writeValueAsString(userData))
                )
                .andExpect(matcher);
    }


    private MvcResult login(
            String username,
            String password,
            ResultMatcher matcher,
            boolean correctLogin) throws Exception{
        final UserData userData = new UserData(
                username,
                password,
                null,
                null,
                null
        );
        when(
                accountService.login(
                        eq(userData.getUsername()),
                        eq(userData.getPassword()))
        ).thenReturn(correctLogin ? UUID.randomUUID() : null);

        return mockMvc
                .perform(
                        post("/api/login")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(mapper.writeValueAsString(userData))
                )
                .andExpect(matcher)
                .andReturn();
    }

    private void logout(MockHttpSession session) throws Exception{
        mockMvc
                .perform(
                        get("/api/logout")
                        .session(session)
                )
                .andExpect(status().isOk());
    }

    private void isLoggedIn(MockHttpSession session, boolean isLoggedIn) throws Exception{
        final String username = session.getAttribute("username") != null ?
                                session.getAttribute("username").toString() :
                                "";
        final UUID sessionID = session.getAttribute("sessionID") != null ?
                                (UUID)session.getAttribute("sessionID") :
                                null;
        when(
                accountService.isLoggedIn(
                        eq(username),
                        eq(sessionID))
        ).thenReturn(isLoggedIn);
        mockMvc
                .perform(
                        get("/api/isloggedin")
                                .session(session)
                )
                .andExpect(isLoggedIn ? status().isOk() : status().isUnauthorized());
    }

    @Test
    public void registerCorrect() throws Exception{
        final String username = getUniqueUsername();
        final MvcResult result = register(username, "pass", status().isOk(), true, true);
        final ResponseStatusData answer =
                mapper.readValue(result.getResponse().getContentAsString(), ResponseStatusData.class);
        assertEquals(username, answer.getMessage());
    }


    @Test
    public void registerConflict() throws Exception {
        final String username = getUniqueUsername();
        register(username, "pass", status().isOk(), true, true);
        register(username, "pass", status().isConflict(), false, false);
    }

    @Test
    public void registerEmptyPassword() throws Exception{
        register(getUniqueUsername(),"", status().isBadRequest(), false, false);
    }

    @Test
    public void registerEmptyUsername() throws  Exception{
        register("","passwd", status().isBadRequest(), false, false);
    }


    @Test
    public void getNotFound() throws Exception{
        getAcc(getUniqueUsername(),  null, status().isNotFound());
    }

    @Test
    public void getCorrect() throws Exception{
        final String username = getUniqueUsername();
        final MvcResult result = register(username, "passwd", status().isOk(), true, true);
        logout((MockHttpSession) result.getRequest().getSession());
        getAcc(username, username + "@mail.ru", status().isOk());
    }


    @Test
    public void loginCorrect() throws Exception{
        final String username = getUniqueUsername();
        final MvcResult result1 = register(username, "pwd", status().isOk(),true,true);
        isLoggedIn((MockHttpSession)result1.getRequest().getSession(), true);
        logout((MockHttpSession)result1.getRequest().getSession());
        isLoggedIn((MockHttpSession)result1.getRequest().getSession(), false);
        final MvcResult result2 = login(username, "pwd",status().isOk(), true);
        isLoggedIn((MockHttpSession)result2.getRequest().getSession(), true);
    }


    @Test
    public void loginAccesDenied() throws Exception{
        final String username = getUniqueUsername();
        final MvcResult result1 = register(username, "pwd2", status().isOk(),true,true);
        isLoggedIn((MockHttpSession)result1.getRequest().getSession(), true);
        logout((MockHttpSession)result1.getRequest().getSession());
        isLoggedIn((MockHttpSession)result1.getRequest().getSession(), false);
        final MvcResult result2 = login(username, "pwd3",status().isForbidden(), false);
        isLoggedIn((MockHttpSession)result2.getRequest().getSession(), false);
    }


    @Test
    public void isLoggedIn() throws Exception {
        final String username = getUniqueUsername();
        final MvcResult result1 = register(username, "pwd", status().isOk(),true,true);
        isLoggedIn((MockHttpSession)result1.getRequest().getSession(), true);
        logout((MockHttpSession)result1.getRequest().getSession());
        isLoggedIn((MockHttpSession)result1.getRequest().getSession(), false);
        final MvcResult result2 = login(username, "pwd",status().isOk(), true);
        isLoggedIn((MockHttpSession)result2.getRequest().getSession(), true);
        logout((MockHttpSession)result1.getRequest().getSession());
        isLoggedIn((MockHttpSession)result1.getRequest().getSession(), false);
    }

    @Test
    public void editCorrect() throws Exception{
        final String username = getUniqueUsername();
        final MvcResult result =
                register(username, "passwd", status().isOk(), true, true);
        getAcc(username, username + "@mail.ru", status().isOk());
        edit(
                (MockHttpSession)result.getRequest().getSession(),
                "new" + username + "@mail.ru",
                "newpasswd",
                status().isForbidden(),
                false);
        getAcc(username, "new" + username + "@mail.ru", status().isOk());
        logout((MockHttpSession)result.getRequest().getSession());
        isLoggedIn((MockHttpSession)result.getRequest().getSession(), false);
        final MvcResult result2 = login(username, "newpasswd",status().isOk(), true);
        isLoggedIn((MockHttpSession)result2.getRequest().getSession(), true);
    }

    @Test
    public void editAccessDenied() throws Exception{
        final String username = getUniqueUsername();
        final MvcResult result =
                register(username, "passwd", status().isOk(), true, true);
        logout((MockHttpSession)result.getRequest().getSession());
        edit(
                (MockHttpSession)result.getRequest().getSession(),
                "new@mail.ru",
                "newpasswd",
                status().isForbidden(),
                false);
    }
}