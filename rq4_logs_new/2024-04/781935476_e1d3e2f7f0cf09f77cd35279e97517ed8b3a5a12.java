
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Monthy {

    @Data
    @AllArgsConstructor
    private static class GameResult {
        private boolean win;


        private static final int NUM_TRIALS = 1000;

        public static void main(String[] args) {
            List<GameResult> resultsWithoutSwitching = new ArrayList<>();
            List<GameResult> resultsWithSwitching = new ArrayList<>();

            for (int i = 0; i < NUM_TRIALS; i++) {
                boolean initialChoice = makeInitialChoice();
                boolean revealedDoor = revealDoor(initialChoice);
                boolean finalChoiceWithoutSwitching = initialChoice;
                boolean finalChoiceWithSwitching = switchDoor(initialChoice, revealedDoor);

                GameResult resultWithoutSwitching = playGame(initialChoice, revealedDoor, finalChoiceWithoutSwitching);
                GameResult resultWithSwitching = playGame(initialChoice, revealedDoor, finalChoiceWithSwitching);

                resultsWithoutSwitching.add(resultWithoutSwitching);
                resultsWithSwitching.add(resultWithSwitching);
            }

            printAndSaveStatistics(resultsWithoutSwitching, "Без смены выбора");
            printAndSaveStatistics(resultsWithSwitching, "Со сменой выбора");
        }

        private static boolean makeInitialChoice() {
            Random random = new Random();
            return random.nextBoolean();
        }

        private static boolean revealDoor(boolean initialChoice) {
            Random random = new Random();
            int revealedDoor = random.nextInt(3);
            while (revealedDoor == (initialChoice ? 1 : 0) || revealedDoor == (initialChoice ? 0 : 1)) {
                revealedDoor = random.nextInt(3);
            }
            return revealedDoor == 1;
        }

        private static boolean switchDoor(boolean initialChoice, boolean revealedDoor) {
            return !initialChoice && !revealedDoor;
        }

        private static GameResult playGame(boolean initialChoice, boolean revealedDoor, boolean finalChoice) {
            return new GameResult(finalChoice && !revealedDoor);
        }

        private static void printAndSaveStatistics(List<GameResult> results, String strategy) {
            long wins = results.stream().filter(GameResult::isWin).count();
            long losses = NUM_TRIALS - wins;

            System.out.println("Стратегия: " + strategy);
            System.out.println("Победы: " + wins);
            System.out.println("Поражения: " + losses);

            // Сохранение результатов в файл локально
            String fileName = "Результаты_игры_" + strategy.replace(" ", "_") + ".txt";
            try (FileWriter writer = new FileWriter(fileName)) {
                for (GameResult result : results) {
                    writer.write(result.isWin() ? "Победа" : "Поражение");
                    writer.write(System.lineSeparator());
                }
                System.out.println("Результаты для '" + strategy + "' сохранены в файл '" + fileName + "'");
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println();
        }
    }
}