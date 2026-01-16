package ManageFiles;

import Classes.GoalsPlayer;
import Classes.Player;
import Classes.Team;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by ignacioojanguren on 14/11/16.
 */
public class ReadComunioTeam {
    public ReadComunioTeam(){}

    private static BufferedReader readContent(String fileName){
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
     * This class obtains from every player all the goals he has scored.
     * @param fileName
     * @return
     */
    public static ArrayList<String> getPlayers(String fileName){
        ArrayList<String> playersComunio = new ArrayList<String>();
        String line;

        BufferedReader br = readContent(fileName);

        try{
            while((line = br.readLine()) != null){
                playersComunio.add(line);
            }
        }catch(IOException ex){
            ex.printStackTrace();
        }finally{
            try{
                if (br != null)br.close();
            }catch (IOException close){
                close.printStackTrace();
            }
        }

        return playersComunio;
    }
}