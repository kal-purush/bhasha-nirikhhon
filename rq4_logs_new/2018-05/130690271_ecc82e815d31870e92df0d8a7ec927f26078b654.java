package pl.gacik.tictac;

import pl.gacik.tictac.languages.MessagesProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class GameInitializer {

    private GameSettings gameSettings;

    public GameInitializer(GameSettings gameSettings) {
        this.gameSettings = gameSettings;
    }

    public void setBoard(Consumer<String> out, Supplier<String> in) {
        if (in == null) {
            throw new IllegalArgumentException("Supplier cannot be null");
        }
        try {
            out.accept(gameSettings.getMessagesProvider().askForBoardWidth());
            int width = Integer.parseInt(in.get());
            out.accept(gameSettings.getMessagesProvider().askForBoardHeight());
            int height = Integer.parseInt(in.get());
            gameSettings.setBoard(new LimitedCrossIBoard(width,height));
        } catch (Exception e) {
            throw e; // TODO: Make any handling exceptions
        }
    }

    public void setLanguage(Consumer<String> out, Supplier<String> in) {
        if (in == null) {
            throw new IllegalArgumentException("Supplier cannot be null");
        }
        try {
            out.accept("CHOOSE LANGUAGE [NUMBER] / WYBIERZ JĘZYK [NUMER]");
            int i=0;
            Map<Integer, MessagesProvider.AVAILABLE_LANGUAGE> tempLanguageMap = new HashMap<>();
            for(MessagesProvider.AVAILABLE_LANGUAGE language : MessagesProvider.AVAILABLE_LANGUAGE.values()) {
                out.accept(i + ". " + language.getLocale());
                tempLanguageMap.put(i, language);
                i++;
            }
            int choose = Integer.parseInt(in.get());
            if (choose<0 || choose>=i) {
                throw new IndexOutOfBoundsException("Your choice is not in available range");
            }
            gameSettings.setMessagesProvider(new MessagesProvider(tempLanguageMap.get(i)));
        } catch (Exception e) {
            throw e; // TODO: Make any handling exceptions
        }
    }


}