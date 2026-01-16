package spike;

import java.util.ArrayList;

public class AnalyzeTranscript {

    static ArrayList<Transcript> transcripts = ConfigReader.getTranscripts();
    //count number of student per year
    public static String countNumberOfStudentPerYear(int year){
        int count = 0;
        for (int i=0;i<transcripts.size();i++){
            boolean found = false;
            Transcript transcript = transcripts.get(i);
            for (int j = 0; j<transcript.getCourses().size()&&!found; j++){
                if (year == transcript.getCourses().get(j).getSection().getYear()){
                    count++;
                    found = true;
                    System.out.println(transcript.getCourses().get(i));
                    System.out.println(i+1);
                }
            }
        }
        return year + ": " + count;
    }

}