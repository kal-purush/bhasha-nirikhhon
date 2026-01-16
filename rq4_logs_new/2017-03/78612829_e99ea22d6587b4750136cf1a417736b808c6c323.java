
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;

/**
 * Created by rabboni on 15/2/17.
 */

/**
 * The program prints out list of active S2IDs matching the user entered criteria.
 * The file name and location of the input file should be changed before running the program.
 *
 * This implementation can be made cleaner and more efficient.
 * The regex check can be better.
 */
public class FindActiveS2IDs {

    private static Pattern SEDOL_PATTERN = Pattern.compile("\\*|SEDOL|\\*");
    private static Pattern CUSIP_PATTERN = Pattern.compile("\\*|CUSIP|\\*");
    private static Pattern ISIN_PATTERN = Pattern.compile("\\*|ISIN|\\*");

    private static final String INPUT_DATE_FORMAT = "yyyyMMdd";

    public static void main(String[] args){

        try {
            BufferedReader userInputBufferedReader = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter the AssetIDType: Input should be SEDOL or CUSIP or ISIN");
            String assetIDType = userInputBufferedReader.readLine();
            Pattern pattern = getAssetIDPattern(assetIDType);

            System.out.println("Enter comma separated list of asset ids:");
            String assetIDs = userInputBufferedReader.readLine();
            String[] assetIDsArray = assetIDs.split(",");

            System.out.println("Enter the date to check for active IDs(Date should be in yyyyMMdd format): ");
            String dateString = userInputBufferedReader.readLine();


            BufferedReader fileBufferedReader = new BufferedReader(new FileReader("/home/rabboni/FindActiveS2IDs/src/IDMapping.txt"));

            Set<String> uniqueActiveS2IDs = findActiveIDs(fileBufferedReader, pattern, assetIDsArray, dateString);


            //Print out unique S2IDs. An extra comma will be printed at the end.
            System.out.println("S2IDs that match the given Asset type, id and active date: ");
            for (String s: uniqueActiveS2IDs){
                System.out.print(s + ",");
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    /**
     * This method returns a set of unique active S2IDs
     * for a given array of assetIDs and input date from user.
     * @param bufferedReader
     * @param pattern
     * @param assetIDsArray
     * @param inputDateString
     * @return
     * @throws IOException
     * @throws ParseException
     */
    public static Set<String> findActiveIDs(BufferedReader bufferedReader, Pattern pattern, String[] assetIDsArray, String inputDateString) throws IOException, ParseException {

        Set<String> hashSet = new HashSet<>();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(INPUT_DATE_FORMAT);
        Date inputDate = simpleDateFormat.parse(inputDateString);

        String line;

        while ((line = bufferedReader.readLine())!= null){
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()){

                //split the line
                String[] lineBlocks = line.split("\\|");

                //System.out.println("Line blocks: " + lineBlocks);

                for (int i = 0; i < assetIDsArray.length; i++){

                    //System.out.println("assetIDsArray[i]: " + assetIDsArray[i]);

                    if (lineBlocks[2].equalsIgnoreCase(assetIDsArray[i].trim())){
                        //Get the start date and endate
                        Date startDate = simpleDateFormat.parse(lineBlocks[3]);
                        Date endDate = simpleDateFormat.parse(lineBlocks[4]);

                        if (inputDate.after(startDate) && inputDate.before(endDate)){

                            //Add the S2ID to the hasset
                            hashSet.add(lineBlocks[0]);
                        }

                    }
                }
            }
        }

        return hashSet;

    }

    /* Helper method to get the asset id pattern given string user input */
    private static Pattern getAssetIDPattern(String userInput){

        switch (userInput){
            case "SEDOL":
                return SEDOL_PATTERN;
            case "CUSIP":
                return CUSIP_PATTERN;
            case "ISIN":
                return ISIN_PATTERN;
            default:
                System.out.println("Assert ID Pattern is not of given type.");
                System.exit(0);
                return null;
        }

    }
}