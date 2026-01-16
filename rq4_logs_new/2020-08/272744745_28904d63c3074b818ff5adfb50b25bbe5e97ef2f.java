package com.dungeongroupfinder.messages;

public class ErrorMessages {

    public static String getErrorMessage(ErrorType messageType) {
        switch(messageType){
            case NO_PERMISSION:
                return "You don't have permission to perform this action.";
            case PLAYER_ID_DOESNT_EXIST:
                return "A player with given id doesn't exist.";
            case GUILD_ID_DOESNT_EXIST:
                return "A guild with given id doesn't exist.";
            case PASSWORDS_DONT_MATCH:
                return "Given passwords don't match each other";
            case PLAYER_HAS_ALREADY_GUILD:
                return "This player has a guild already.";
            default:
                return "An error occurred.";
        }
    }
}