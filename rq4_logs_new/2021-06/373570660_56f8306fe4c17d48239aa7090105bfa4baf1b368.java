package me.luucx7.creativecore.v2.api;

public interface Quiz {

    boolean isRunning();
    boolean addAnswer(Resposta resposta);

    void setRightAnswer(RespostaCerta rightAnswer);

    void start();
    void stop();
}