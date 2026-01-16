package com.example.backend.dtos;

import com.example.backend.services.PowerUp;

import java.util.List;

public class PowerUpDTO {

    private String error;

    private List<PowerUp> powerUp;

    public PowerUpDTO() {
    }

    public PowerUpDTO(String error, List<PowerUp> powerUp) {
        this.error = error;
        this.powerUp = powerUp;
    }

    public PowerUpDTO(String localizedMessage) {
        this.error = localizedMessage;
    }

    public String getError() {
        return error;
    }

    public List<PowerUp> getPowerUp() {
        return powerUp;
    }

    public void setError(String error) {
        this.error = error;
    }

    public void setPowerUp(List<PowerUp> powerUp) {
        this.powerUp = powerUp;
    }

}