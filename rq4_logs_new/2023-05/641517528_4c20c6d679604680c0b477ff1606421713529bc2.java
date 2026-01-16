package com.icoffiel.games.business;

import com.icoffiel.games.db.GameEntity;
import com.icoffiel.games.db.GameEntityRepository;
import com.icoffiel.games.rest.AddGameRequest;
import com.icoffiel.games.rest.GameResponse;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
public class GameService {
    private final GameEntityRepository gameRepository;

    public GameService(GameEntityRepository gameRepository) {
        this.gameRepository = gameRepository;
    }

    public List<GameResponse> getGames() {
        return gameRepository
                .findAll().stream()
                .map(game -> new GameResponse(
                        game.getId(),
                        game.getName(),
                        game.getDescription(),
                        game.getNumberOfPlayers(),
                        game.getPrice(),
                        game.getSystemId(),
                        game.getReleaseDate()
                ))
                .toList();
    }

    public Optional<GameResponse> getGame(Long id) {
        return gameRepository
                .findById(id)
                .map(game -> new GameResponse(
                        game.getId(),
                        game.getName(),
                        game.getDescription(),
                        game.getNumberOfPlayers(),
                        game.getPrice(),
                        game.getSystemId(),
                        game.getReleaseDate()
                ));
    }

    public GameResponse createGame(AddGameRequest addGameRequest) {
        // TODO - validate the system is a known system. Ingest systems from a kafka stream and save to a DB

        GameEntity gameEntity = new GameEntity();
        gameEntity.setName(addGameRequest.name());
        gameEntity.setDescription(addGameRequest.description());
        gameEntity.setNumberOfPlayers(addGameRequest.numberOfPlayers());
        gameEntity.setPrice(addGameRequest.price());
        gameEntity.setSystemId(addGameRequest.systemId());
        gameEntity.setReleaseDate(addGameRequest.releaseDate());

        GameEntity savedGameEntity = gameRepository.save(gameEntity);

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
    public void deleteGame(Long id) {
        gameRepository.deleteById(id);
    }
}