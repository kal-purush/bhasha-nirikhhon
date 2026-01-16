package com.tikal.fuze.antscosmashing.scoreservice.controller;

import com.amazonaws.services.kinesisfirehose.model.InvalidArgumentException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tikal.fuze.antscosmashing.scoreservice.service.PlayerScoresService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

public class GetScoresWebApiHandler implements RequestStreamHandler {
    private static final Logger logger = LogManager.getLogger(GetScoresWebApiHandler.class);
    private ObjectMapper om = new ObjectMapper();
    private PlayerScoresService playerScoresService;

    public GetScoresWebApiHandler() {
        if (playerScoresService == null)
            playerScoresService = new PlayerScoresService();
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)  {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            JsonNode event = om.readTree(reader);
            JsonNode queryStringParameters = event.get("queryStringParameters");
            String gameId = queryStringParameters.get("gameId").textValue();
            String type = queryStringParameters.get("type").textValue();
            String scores;
            if(type.equals("player"))
                scores = playerScoresService.getPlayersScores(gameId);
            else if(type.equals("team"))
                scores = playerScoresService.getTeamsScores(gameId);
            else
                throw new InvalidArgumentException("wrong type: "+type);

            OutputStreamWriter writer = new OutputStreamWriter(outputStream, "UTF-8");
            ObjectNode result = om.createObjectNode()
                    .put("statusCode", 200)
                    .put("body", scores);
            writer.write(result.toString());
            writer.close();
        } catch (Exception e) {
            logger.error("Failed to process event",e);
            throw new RuntimeException(e);
        }
    }
}