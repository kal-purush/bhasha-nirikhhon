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

import Transport4Future.TokenManagement.database.skeleton.Database;
import Transport4Future.TokenManagement.exception.TokenManagementException;
import Transport4Future.TokenManagement.model.Token;
import Transport4Future.TokenManagement.model.TokenRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TokenRequestDatabase implements Database {

    private static TokenRequestDatabase tokenDatabase;
    private List<Token> tokensList;

    private TokenRequestDatabase() {

    }

    public static TokenRequestDatabase getInstance() {
        if (tokenDatabase == null) {
            tokenDatabase = new TokenRequestDatabase();
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

    public void saveTokenRequest(TokenRequest tokenRequest, String hex) throws TokenManagementException {

        //Generar un HashMap para guardar los objetos
        Gson gson = new Gson();
        String jsonString;
        HashMap<String, TokenRequest> clonedMap;
        String storePath = System.getProperty("user.dir") + "/Store/tokenRequestsStore.json";
        //Tengo que cargar el almacen de tokens request en memoria y añadir el nuevo si no existe
        try {
            Object object = gson.fromJson(new FileReader(storePath), Object.class);
            jsonString = gson.toJson(object);
            Type type = new TypeToken<HashMap<String, TokenRequest>>() {
            }.getType();
            clonedMap = gson.fromJson(jsonString, type);
        } catch (Exception e) {
            clonedMap = null;
        }
        if (clonedMap == null) {
            clonedMap = new HashMap();
            clonedMap.put(hex, tokenRequest);
        } else if (!clonedMap.containsKey(hex)) {
            clonedMap.put(hex, tokenRequest);
        }


        // Guardar el Tokens Requests Store actualizado
        jsonString = gson.toJson(clonedMap);
        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(storePath);
            fileWriter.write(jsonString);
            fileWriter.close();
        } catch (IOException e) {
            throw new TokenManagementException("Error: Unable to save a new token in the internal licenses store");
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