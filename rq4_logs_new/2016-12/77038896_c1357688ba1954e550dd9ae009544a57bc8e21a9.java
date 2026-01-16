package com.codingblocks.codequiz.api;

import com.codingblocks.codequiz.dummy_utils.DummyUser;
import com.codingblocks.codequiz.models.User;
import com.google.gson.Gson;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;

import java.util.ArrayList;

/**
 * Created by piyush0 on 27/12/16.
 */
public class UserHandler extends RoutingHandler {

    public UserHandler() {
        super();
        this.get("/list", this::GET_users);
    }

    private void GET_users(HttpServerExchange exchange) throws Exception {
        ArrayList<User> users = DummyUser.getUsers();

        Gson gson = new Gson();
        exchange.getResponseSender().send(gson.toJson(users));
    }
}