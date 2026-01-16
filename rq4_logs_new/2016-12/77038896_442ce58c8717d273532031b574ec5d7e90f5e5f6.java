package com.codingblocks.codequiz.api;

import com.codingblocks.codequiz.dummy_utils.DummyQuestions;
import com.codingblocks.codequiz.models.Question;
import com.codingblocks.codequiz.models.User;
import com.codingblocks.codequiz.utils.ReadQuery;
import com.google.gson.*;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;

import javax.jws.soap.SOAPBinding;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Created by piyush0 on 30/12/16.
 */
public class ChallengeHandler extends RoutingHandler {
    public ChallengeHandler(){
        super();
        this.post("/create", this::POST_createChallenge);
        this.get("/{challengeId}", this::GET_getChallengeQuestions);
    }

    private void GET_getChallengeQuestions(HttpServerExchange exchange) throws Exception{

        int id = Integer.valueOf(exchange.getQueryParameters().get("challengeId").getFirst());

        ArrayList<Question> questions = getQuestionsBasedOnCid(id);
        Gson gson = new Gson();

        exchange.getResponseSender().send(gson.toJson(questions));
    }

    private ArrayList<Question> getQuestionsBasedOnCid(Integer id) {
        return DummyQuestions.getDummyQuestions(); //TODO: Make this better.
    }

    private void POST_createChallenge(HttpServerExchange exchange) throws Exception{
        if (exchange.isInIoThread()) {
            exchange.dispatch(this::POST_createChallenge);
            return;
        }
        Gson gson = new Gson();
        JsonObject object = (JsonObject) new JsonParser().parse(ReadQuery.read(exchange));

        Integer numQues = gson.fromJson(object.get("numQues"), Integer.class);
        ArrayList<User> users = getUsers(object.getAsJsonArray("users"), gson);
        String topic = gson.fromJson(object.get("topic"), String.class);

        int cId = getChallengeId(users, numQues, topic);

        exchange.getResponseSender().send(gson.toJson(cId));

    }

    private ArrayList<User> getUsers(JsonArray users, Gson gson){
        ArrayList<User> retVal = new ArrayList<>();

        for(int i = 0 ; i<users.size(); i++) {
            User u = gson.fromJson(users.get(i),User.class);
            retVal.add(u);
        }

        return retVal;
    }

    private int getChallengeId(ArrayList<User> users, Integer numQues, String topic){
        return 1; // TODO: Make this better.
    }

}