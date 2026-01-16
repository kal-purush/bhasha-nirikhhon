package com.fernando;

import com.fernando.interceptor.Logged;
import com.fernando.model.Game;
import com.fernando.model.GameInfo;
import com.fernando.service.GameService;
import javax.enterprise.inject.Model;
import javax.inject.Inject;
import java.util.List;

@Model
public class GameStore {

    private String name;
    private double price;
    private int quantity;
    private String producer;

    @Inject
    private GameService gameManager;

    @Logged
    public void addGameInfo() {
        gameManager.addGame(new GameInfo(name, price, quantity, producer));
    }
    public void removeGame(){
        gameManager.removeGame(new GameInfo(name, price, quantity, producer));
    }
    public void sortGames(){
        gameManager.sortGames();
    }

    public List<Game> getGameList(){
        return gameManager.getGames();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getProducer() {
        return producer;
    }

    public void setProducer(String producer) {
        this.producer = producer;
    }

    public GameService getGameManager() {
        return gameManager;
    }

    public void setGameManager(GameService gameManager) {
        this.gameManager = gameManager;
    }
}