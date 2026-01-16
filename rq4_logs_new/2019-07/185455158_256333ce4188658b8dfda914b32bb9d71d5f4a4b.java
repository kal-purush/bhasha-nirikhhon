package com.hotsauce.meem.state;

public class GreetingContext {

    private ActiveGreetingState activeState;
    private IntroGreetingState introState;
    private GreetingState currentState;

    public GreetingContext() {
        activeState = new ActiveGreetingState(this);
        introState = new IntroGreetingState(this);
        currentState = introState;
    }

    public String getGreetingsString(int numberOfMeems) {
        return currentState.getGreetingsString(numberOfMeems);
    }

    public void transitionToActive() {
        currentState = activeState;
    }

    public void transitionToIntro() {
        currentState = introState;
    }


}