package dk.dtu.compute.se.pisd.roborally.fileaccess;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import dk.dtu.compute.se.pisd.roborally.controller.FieldAction;
import dk.dtu.compute.se.pisd.roborally.fileaccess.model.BoardTemplate;
import dk.dtu.compute.se.pisd.roborally.fileaccess.model.CommandCardFieldTemplate;
import dk.dtu.compute.se.pisd.roborally.fileaccess.model.PlayerTemplate;
import dk.dtu.compute.se.pisd.roborally.fileaccess.model.SpaceTemplate;
import dk.dtu.compute.se.pisd.roborally.model.Board;
import dk.dtu.compute.se.pisd.roborally.model.Player;
import dk.dtu.compute.se.pisd.roborally.model.Space;

import java.util.ArrayList;
import java.util.List;

public class SerializeGame {
    public static String serializeGame(Board board) {
        BoardTemplate boardTemplate = new BoardTemplate();
        boardTemplate.width = board.width;
        boardTemplate.height = board.height;
        boardTemplate.phase = board.getPhase();
        boardTemplate.current = board.getPlayerNumber(board.getCurrentPlayer());
        boardTemplate.priorityAntenna = new SpaceTemplate();
        boardTemplate.priorityAntenna.x = board.getPriorityAntenna().x;
        boardTemplate.priorityAntenna.y = board.getPriorityAntenna().y;
        boardTemplate.priorityAntenna.actions = board.getPriorityAntenna().getActions();
        boardTemplate.priorityAntenna.walls = board.getPriorityAntenna().getWalls();
        boardTemplate.numOfCheckpoints = board.getNumOfCheckpoints();

        for (int i = 0; i < board.width; i++) {
            for (int j = 0; j < board.height; j++) {
                Space space = board.getSpace(i, j);
                if (!space.getWalls().isEmpty() || !space.getActions().isEmpty()) {
                    SpaceTemplate spaceTemplate = new SpaceTemplate();
                    spaceTemplate.x = space.x;
                    spaceTemplate.y = space.y;
                    spaceTemplate.actions.addAll(space.getActions());
                    spaceTemplate.walls.addAll(space.getWalls());
                    boardTemplate.spaces.add(spaceTemplate);
                }
            }
        }

        List<PlayerTemplate> playerTemplates = new ArrayList<>();
        for (int i = 0; i < board.getPlayersNumber(); i++) {
            Player player = board.getPlayer(i);
            PlayerTemplate playerTemplate = new PlayerTemplate();
            playerTemplate.name = player.getName();
            playerTemplate.color = player.getColor();
            playerTemplate.space = new SpaceTemplate();
            playerTemplate.space.walls = player.getSpace().getWalls();
            playerTemplate.space.actions = player.getSpace().getActions();
            playerTemplate.space.x = player.getSpace().x;
            playerTemplate.space.y = player.getSpace().y;
            playerTemplate.heading = player.getHeading();
            playerTemplate.program = new CommandCardFieldTemplate[5];
            for (int k = 0; k < playerTemplate.program.length; k++) {
                playerTemplate.program[k] = new CommandCardFieldTemplate();
                playerTemplate.program[k].card = player.getProgramField(k).getCard();
                playerTemplate.program[k].visible = player.getProgramField(k).isVisible();
            }
            playerTemplate.cards = new CommandCardFieldTemplate[8];
            for (int j = 0; j < playerTemplate.cards.length; j++) {
                playerTemplate.cards[j] = new CommandCardFieldTemplate();
                playerTemplate.cards[j].card = player.getCardField(j).getCard();
                playerTemplate.cards[j].visible = player.getCardField(j).isVisible();
            }
            playerTemplates.add(playerTemplate);
        }

        boardTemplate.players = playerTemplates;

        GsonBuilder simpleBuilder = new GsonBuilder().
                registerTypeAdapter(FieldAction.class, new Adapter<FieldAction>()).
                setPrettyPrinting();
        Gson gson = simpleBuilder.create();

        return gson.toJson(boardTemplate, boardTemplate.getClass());
    }
}