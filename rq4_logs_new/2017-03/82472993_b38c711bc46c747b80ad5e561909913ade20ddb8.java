package com.kvteam.backend.dataformats;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.UUID;

/**
 * Created by maxim on 28.03.17.
 */
// Отправляется только сервер -> клиент
public class ActionData {
    @NotNull
    @JsonProperty("unitID")
    private UUID unitID;
    @NotNull
    @JsonProperty("actiontType")
    private String actionType;
    @NotNull
    @JsonProperty("actionParameters")
    private HashMap<String, Object> actionParameters;
    @JsonProperty("timeOffsetBegin")
    private int timeOffsetBegin;
    @JsonProperty("timeOffsetEnd")
    private int timeOffsetEnd;


    public ActionData(@NotNull UUID unitID,
                      @NotNull String actionType,
                      @NotNull HashMap<String, Object> actionParameters,
                      int timeOffsetBegin,
                      int timeOffsetEnd){
        this.unitID = unitID;
        this.actionType = actionType;
        this.actionParameters = actionParameters;
        this.timeOffsetBegin = timeOffsetBegin;
        this.timeOffsetEnd = timeOffsetEnd;
    }

}