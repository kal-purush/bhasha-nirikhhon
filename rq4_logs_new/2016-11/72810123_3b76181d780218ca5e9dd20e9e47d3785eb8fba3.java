package Calculation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by ignacioojanguren on 5/11/16.
 *
 * This class analyze the document goalPlayers and obtain the data that is useful for us.
 * The class is external to the other classes, it has its own main method as we only want to clean the html obtained from
 * a web page.
 * The program gets the information of the soccer player, the goals he scored and also how long it takes a player
 * to score a goal.
 */
public class ObtainGoals {

    private static BufferedReader readContent(){

        String fileName = "File";
        BufferedReader br = null;
        try{
            br = new BufferedReader(new FileReader(fileName));
        }catch (FileNotFoundException ex){
            ex.printStackTrace();
            return null;
        }
        return br;
    }


    /**
     * This method casts the information of the file goalPlayers, and it prints on console the information of the players.
     * The information is stored in a finalGoalPlayers.txt.
     */
    public static void getMatches(){

        BufferedReader br = readContent();
        String line;
        String[] lineSplit = {""};
        String name;
        String goals;
        String minutes;
        String sentenceCleaned;
        int cnt = 0;

        try{
            PrintWriter writer = new PrintWriter
                    ("File", "UTF-8");
            while( (line = br.readLine()) != null){
                lineSplit = line.split(" ");
                sentenceCleaned = "";
                try{
                    if(lineSplit.length>2){
                        if(lineSplit[1].equals("<td") && lineSplit.length>3){
                            if(lineSplit[2].equals("class=\"nombre_1em\"")){
                                name = lineSplit[5];
                                if(lineSplit.length==8){
                                    name += " " + lineSplit[6];
                                }
                                sentenceCleaned += name;
                            }
                            if(lineSplit[2].equals("class=\"principal\"")){
                                goals = ";" + lineSplit[4];
                                sentenceCleaned += goals;
                            }
                            if(lineSplit[2].equals("class=\"nombre_08em\"")){
                                minutes = ";" + lineSplit[5];
                                sentenceCleaned += minutes;
                                cnt ++;
                                if (cnt == 2){
                                    sentenceCleaned += ";\n";
                                    cnt = 0;
                                }
                            }
                            writer.write(sentenceCleaned);
                        }
                    }


                }catch(NumberFormatException nf){
                    nf.printStackTrace();
                }
            }
            writer.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }finally{
            try{
                if (br != null)br.close();
            }catch (IOException close){
                close.printStackTrace();
            }
        }
    }

    public static void main(String[]args){
        getMatches();
    }

}