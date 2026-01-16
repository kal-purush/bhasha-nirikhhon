package org.openjfx;

import java.util.HashMap;

import org.openjfx.otherClasses.NewGreenCard;

public class ExternalDatabase {
    private HashMap<String, NewGreenCard> externDatabase = new HashMap<String, NewGreenCard>();

    public ExternalDatabase() {
        NewGreenCard test = NewGreenCard.createNewGreenCard("1829 Lois Lane", "Brian Andres", "ABC123456789",
                "brian@gmail.com");

        externDatabase.put("ABC123456789",
                NewGreenCard.createNewGreenCard("1829 Lois Lane", "Brian Andres", "ABC123456789", "brian@gmail.com"));
        externDatabase.put("DEF123456789", NewGreenCard.createNewGreenCard("6262 Jane Forest", "Winston Sheng",
                "DEF123456789", "winston@gmail.com"));
        externDatabase.put("GHI123456789",
                NewGreenCard.createNewGreenCard("6392 Gmu Lane", "Bianca Akins", "GHI123456789", "bianca@gmail.com"));
        externDatabase.put("JKL123456789",
                NewGreenCard.createNewGreenCard("2341 Pillow Lane", "John Smith", "JKL123456789", "john@gmail.com"));
        externDatabase.put("MNO123456789",
                NewGreenCard.createNewGreenCard("1242 Pillow Lane", "Jane Doe", "MNO123456789", "jane@gmail.com"));
        externDatabase.put("PQR123456789",
                NewGreenCard.createNewGreenCard("8952 Couch Road", "Glass Fellow", "PQR123456789", "Glass@gmail.com"));

    }

    public NewGreenCard searchExternalDatabase(String id) {

        return externDatabase.get(id);

    }

}