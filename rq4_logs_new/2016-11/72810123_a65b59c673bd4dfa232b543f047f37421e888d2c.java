package ManageFiles;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by ignacioojanguren on 14/11/16.
 */
public class CastContentMatches {

    final static String fileName = "/Users/ignacioojanguren/IdeaProjects/StatisticsFutbol/src/text/ContentHtmlMatches";
    final static String fileToWrite = "/Users/ignacioojanguren/IdeaProjects/StatisticsFutbol/src/text/MatchInfoTest.txt";
    private static BufferedReader readContent(){
        BufferedReader br = null;
        try{
            br = new BufferedReader(new FileReader(fileName));
        }catch (FileNotFoundException ex){
            ex.printStackTrace();
            return null;
        }
        return br;
    }


    public static void cleanDocument(){
        BufferedReader br = readContent();

        String line, jornada = "", dateMatch ="", result = "", localTeam = "", visitantTeam = "", scorers ="", prevLocalTeam="";
        String[] lineSplit,subSplit, infoMatch = new String[7];
        ArrayList<String[]> allMatches = new ArrayList<String[]>();


        try{
            while((line = br.readLine()) != null){
                lineSplit = line.split(" ");
                if(lineSplit.length>2){
                    if(lineSplit[1].equals("class=\"tit-modulo\">Jornada")){
                        jornada = "# Jornada "+lineSplit[2];
                    }else if (lineSplit[1].equals("itemprop=\"startDate\"")){
                        lineSplit = lineSplit[2].split("\"");
                        lineSplit = lineSplit[1].split("T");
                        dateMatch = lineSplit[0];
                    }else if(arrayContains(lineSplit,"class=\"resultado\">")){
                        if(lineSplit.length == 13){
                            prevLocalTeam = localTeam;
                            subSplit = lineSplit[2].split("\"");
                            if(!lineSplit[3].equals("-"))
                                localTeam = subSplit[1]+ " "+lineSplit[3];
                            visitantTeam = lineSplit[5];
                            result = lineSplit[9]+lineSplit[10]+lineSplit[11] + ";";
                        }else if(lineSplit.length>13){
                            prevLocalTeam = localTeam;
                            subSplit = lineSplit[2].split("\"");
                            if(!lineSplit[3].equals("-"))
                                localTeam = subSplit[1] + " "+ lineSplit[3];
                            visitantTeam = lineSplit[5] + " "+lineSplit[6];
                            result = lineSplit[10]+lineSplit[11]+lineSplit[12] + ";";
                        }else{
                            prevLocalTeam = localTeam;
                            subSplit = lineSplit[2].split("\"");
                            localTeam = subSplit[1];
                            visitantTeam = lineSplit[4];
                            result = lineSplit[8]+lineSplit[9]+lineSplit[10];
                        }
                    }else if(arrayContains(lineSplit,"class=\"nombre\"") && arrayContains(lineSplit, "<strong>")){
                        if(lineSplit.length == 13) {
                            subSplit = lineSplit[11].split("<");
                            scorers += ","+subSplit[0];
                        }else if(lineSplit.length == 15){
                            subSplit = lineSplit[13].split("<");
                            scorers += ","+ lineSplit[12]+ " "+ subSplit[0];
                        }
                    }else if(lineSplit[2].equals("itemtype=\"http://schema.org/SportsEvent\"")){
                        infoMatch[0] = jornada;
                        infoMatch[1] = dateMatch;
                        infoMatch[2] = "2016-2017";
                        infoMatch[3] = localTeam;
                        infoMatch[4] = visitantTeam;
                        infoMatch[5] = result;
                        infoMatch[6] = scorers;
                        allMatches.add(infoMatch);
                        infoMatch = new String[7];
                        scorers = "";
                    }

                }
            }

        }catch (IOException ex){
            System.out.print("Couldn't find file: " + fileName);
        }

        writeInformation(allMatches);

    }

    public static void writeInformation(ArrayList<String[]>infoMatches){
        PrintWriter writer = null;
        String jornada = "";
        try{
            writer = new PrintWriter(fileToWrite, "UTF-8");
            for(String[] element:infoMatches){
                if(!jornada.equals(element[0])){
                    writer.print(element[0] + "\n");
                    jornada = element[0];
                }
                if(!element[1].equals("")){
                    writer.print(element[1] + ";" + element[2] + ";" +element[3]+";"+element[4]+";"+element[5]+";"+element[6] + "\n");
                }
            }
            writer.println("The first line");
            writer.println("The second line");
        }catch (IOException ex){
            ex.printStackTrace();
        }
        writer.close();
    }

    public static boolean arrayContains(String[]array, String valueSearch){

        for(int i = 0; i < array.length;i++){
            if(array[i].equals(valueSearch)){
                return true;
            }
        }

        return false;
    }

    public static void main(String[]args){
        cleanDocument();
    }
}