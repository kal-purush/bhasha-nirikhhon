package org.vashonsd.Games;

import org.vashonsd.Utils.Minigame;

public class PersonalityGame extends Minigame {

    Personality personality;

    public PersonalityGame() {
        super("PersonalityGame", "Change a personality", "quit");
        personality = new Demon();
    }

    @Override
    public String start() {
        return "You see an ambiguous person in front of you." +
                "\nDecide if you will greet, challenge, or flatter him.";
    }

    @Override
    public String handle(String str) {
        if(str.equals("greet")) {
            return personality.greet();
        } else if(str.equals("challenge")) {
            return personality.challenge();
        } else if(str.equals("flatter")) {
            return personality.flatter();
        } else {
            return "huh?";
        }
    }

        @Override
        public String quit() {
            return "Thanks for playing!";
        }
    }