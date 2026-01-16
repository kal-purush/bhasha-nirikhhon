/*
 * Copyright (c) 2020.
 * Content created by:
 * - Andrei García Cuadra
 * - Miguel Hernández Cassel
 *
 * For the module PDS, on university Carlos III de Madrid.
 * Do not share, review nor edit any content without implicitly asking permission to it's owners, as you can contact by this email:
 * andreigarciacuadra@gmail.com
 *
 * All rights reserved.
 */

package Transport4Future.TokenManagement.database;

import Transport4Future.TokenManagement.exception.TokenManagementException;
import Transport4Future.TokenManagement.model.Token;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TokenDatabase {

    private static TokenDatabase tokenDatabase;
    private List<Token> tokensList;

    private TokenDatabase() {

    }

    public static TokenDatabase getInstance() {
        if (tokenDatabase == null) {
            tokenDatabase = new TokenDatabase();
        }
        return tokenDatabase;
    }

    private void load() {
        try {
            JsonReader reader = new JsonReader(new FileReader(System.getProperty("user.dir") + "/Store/tokenStore.json"));
            Gson gson = new Gson();
            Token[] myArray = gson.fromJson(reader, Token[].class);
            this.tokensList = new ArrayList<Token>();
            for (Token token : myArray) {
                this.tokensList.add(token);
            }
        } catch (Exception ex) {
            this.tokensList = new ArrayList<Token>();
        }
    }

    public void add(Token newToken) throws TokenManagementException {
        this.load();
        if (find(newToken.getTokenValue()) == null) {
            tokensList.add(newToken);
            this.save();
        }
    }

    private void save() throws TokenManagementException {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        String jsonString = gson.toJson(this.tokensList);
        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(System.getProperty("user.dir") + "/Store/tokenStore.json");
            fileWriter.write(jsonString);
            fileWriter.close();
        } catch (IOException e) {
            throw new TokenManagementException("Error: Unable to save a new token in the internal licenses store");
        }
    }

    public Token find(String tokenToFind) {
        Token result = null;
        this.load();
        for (Token token : this.tokensList) {
            if (token.getTokenValue().equals(tokenToFind)) {
                result = token;
            }
        }
        return result;
    }
}