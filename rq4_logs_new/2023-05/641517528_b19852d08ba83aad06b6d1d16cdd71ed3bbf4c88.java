package com.icoffiel.games.business;

import com.icoffiel.games.db.GameEntity;
import com.icoffiel.games.rest.AddGameRequest;
import com.icoffiel.games.rest.GameResponse;
import org.springframework.stereotype.Component;

@Component
public class GameAdapter {

    public GameResponse toGameResponse(GameEntity savedGameEntity) {
        return new GameResponse(
                savedGameEntity.getId(),
                savedGameEntity.getName(),
                savedGameEntity.getDescription(),
                savedGameEntity.getNumberOfPlayers(),
                savedGameEntity.getPrice(),
                savedGameEntity.getSystemId(),
                savedGameEntity.getReleaseDate()
        );
    }

    public GameEntity toGameEntity(AddGameRequest addGameRequest) {
        GameEntity gameEntity = new GameEntity();
        gameEntity.setName(addGameRequest.name());
        gameEntity.setDescription(addGameRequest.description());
        gameEntity.setNumberOfPlayers(addGameRequest.numberOfPlayers());
        gameEntity.setPrice(addGameRequest.price());
        gameEntity.setSystemId(addGameRequest.systemId());
        gameEntity.setReleaseDate(addGameRequest.releaseDate());
        return gameEntity;
    }
}