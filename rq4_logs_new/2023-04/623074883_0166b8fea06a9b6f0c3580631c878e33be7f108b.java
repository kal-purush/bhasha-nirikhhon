package com.groupthree.mancala.util;

import javafx.stage.Stage;

import java.util.List;
import java.util.Stack;

public class ApplicationContextManager {

    private static ApplicationContextManager INSTANCE;
    private final Stack<String> viewStack;
    private final Stack<Stage> stageStack;

    private ApplicationContextManager() {
        viewStack = new Stack<>();
        stageStack = new Stack<>();

    }

    public static ApplicationContextManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ApplicationContextManager();
        }
        return INSTANCE;
    }

    public Stack<Stage> getStageStack() {
        return stageStack;
    }

    public Stack<String> getViewStack() {
        return viewStack;
    }

    public void addStage(Stage stage) {
        stageStack.add(stage);
    }

    public void addView(String view) {
        viewStack.add(view);
    }

    public Stage getPreviousStage() {
        return stageStack.pop();
    }

    public String getPreviousView() {
        return viewStack.pop();
    }
}