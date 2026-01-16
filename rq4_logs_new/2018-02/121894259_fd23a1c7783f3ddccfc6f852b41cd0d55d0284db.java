import domain.ResultDuration;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.time.LocalTime;
import java.util.regex.Matcher;
import service.ResultsHolder;

public class Runner {
    public static void main(String[] args) {
        String GREET_MSG = "enter file name in DD-MM-YYYY format";
        String EXIT_CMD = "exit";
        String INCORRECT_FILE_MSG = "incorrect file name";
        String CORRUPTED_DATA_MSG = "some data was corrupted. Approximate results are listed bellow";
        String OK_DATA_MSG = "data is ok. Correct results are listed bellow";
        String TIME_MSG = "time in a building ";
        String FILE_NOT_FOUND_ERR = "file wasn't found in directory";
        String IO_ERR = "unable to read file properly";
        String GLOBAL_ERR = "some error occured. Maybe file is empty";
        String FINISH_MSG = "finishing....";

        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            ResultsHolder holder = ResultsHolder.getResultsHolderInstance();
            String userInput;
            System.out.println(GREET_MSG);
            while (!(userInput = br.readLine()).equals(EXIT_CMD)) {
                Matcher fileNameMatcher = ResultsHolder.fileNamePattern.matcher(userInput);

                if (!fileNameMatcher.matches()) {
                    throw new InvalidPathException(userInput, INCORRECT_FILE_MSG);
                }

                ResultDuration resultDuration = holder.getResultDurationByDate(userInput);
                LocalTime time = resultDuration.getTime();
                printOutput(CORRUPTED_DATA_MSG, OK_DATA_MSG, TIME_MSG, resultDuration, time);
                System.out.println(GREET_MSG);
            }
        }
        catch (NoSuchFileException e) {
            System.err.println(FILE_NOT_FOUND_ERR);
            System.err.println(FINISH_MSG);
        }
        catch (IOException e) {
            System.err.println(IO_ERR);
            System.err.println(FINISH_MSG);
        }
        catch (InvalidPathException e) {
            System.err.println(e.getInput() + " - " + e.getReason());
            System.err.println(FINISH_MSG);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.err.println(GLOBAL_ERR);
            System.err.println(FINISH_MSG);
        }

        //todo separators part1
    }

    private static void printOutput(String CORRUPTED_DATA_MSG, String OK_DATA_MSG, String TIME_MSG,
                                    ResultDuration resultDuration, LocalTime time) {
        System.out.println(resultDuration.isWithError()
            ? CORRUPTED_DATA_MSG
            : OK_DATA_MSG);
        System.out.println(TIME_MSG
            + time.getHour() + "h"
            + ":" + time.getMinute() + "min");
    }

}