// File name RecordingSort_Addra.java
// Written by Welton Addra
// Written on January 21th 2022
// Instructor: Udeme Aaron 

import java.util.*;

    public class RecordingSort_Addra {
        public static void main(String[] args) {
            String[] titles = new String[5];
            String[] artists = new String[5];
            int[] playTimes = new int[5];
            String inputSort = "";
            boolean wrongInput = true; 
            int x = 0;
            String temp;
            int tempNum;
            Scanner input = new Scanner(System.in);

            Recording_Addra r1 = new Recording_Addra();
            Recording_Addra r2 = new Recording_Addra();
            Recording_Addra r3 = new Recording_Addra();
            Recording_Addra r4 = new Recording_Addra();
            Recording_Addra r5 = new Recording_Addra();
            Recording_Addra[] recordings =  {r1,r2,r3,r4,r5};

            for (Recording_Addra r : recordings) {
                    System.out.print("Enter the recording artist >> ");
                    r.setArtist(input.nextLine());
                    artists[x] = r.getArtist();

                    System.out.print("Enter the recording's title >> ");
                    r.setTitle(input.nextLine());
                    titles[x] = r.getTitle();

                    System.out.print("Enter the playtime >> "); 
                    r.setPlayTime(input.nextInt());
                    playTimes[x] = r.getPlayTime();
                    ++x;
                    System.out.println();
                    input.nextLine();
            }

            
            while(wrongInput){
            System.out.print("How would you like to sort these recordings? Using the recording title, recording artist or playtime? >> ");

            inputSort = input.nextLine();
            if (inputSort.equals("recording title")){
                wrongInput = false;
                for (int j = 0; j < titles.length; j++) {
   	                  for (int i = j + 1; i < titles.length; i++) {
		                if (titles[i].compareTo(titles[j]) < 0) {
			                temp = titles[j];
			                titles[j] = titles[i];
			                titles[i] = temp;

                            temp = artists[j];
			                artists[j] = artists[i];
			                artists[i] = temp;

                            tempNum = playTimes [j];
			                playTimes [j] = playTimes [i];
			                playTimes [i] = tempNum;

		    }
	   }     
    }
}

            else if (inputSort.equals("recording artist")){
                wrongInput = false;
                for (int j = 0; j < artists.length; j++) {
                    for (int i = j + 1; i < artists.length; i++) {
                    if (artists[i].compareTo(artists[j]) < 0) {
                       temp = titles[j];
                       titles[j] = titles[i];
                       titles[i] = temp;

                       temp = artists[j];
                       artists[j] = artists[i];
                       artists[i] = temp;

                       tempNum = playTimes [j];
                       playTimes [j] = playTimes [i];
                       playTimes [i] = tempNum;

    }
  }     
}
                            }

            else if (inputSort.equals("playtime")){
                wrongInput = false;
                for (int j = 0; j < playTimes.length; j++) {
                    for (int i = j + 1; i < playTimes.length; i++) {
                    if (playTimes[i] < playTimes[j]) {
                       temp = titles[j];
                       titles[j] = titles[i];
                       titles[i] = temp;

                       temp = artists[j];
                       artists[j] = artists[i];
                       artists[i] = temp;

                       tempNum = playTimes [j];
                       playTimes [j] = playTimes [i];
                       playTimes [i] = tempNum;
   }
}     
            }
        }
            else {
                System.out.println("You entered an invalid value, enter : recording title, recording artist, or playtime");
                
            }
            
        }
        for (x = 0; x < artists.length; ++x){
            System.out.println("Artist : " + artists[x] + "        Recording title : "  + titles[x] + "         Playtime :  " + playTimes[x]);
        
        }   
    } 
}