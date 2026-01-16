import java.util.*;

class Solution {
    static class Info implements Comparable<Info>{
        int idx;
        int playTime;
        String musicName;
        String musicString;

        public Info(int idx, int playTime, String musicName, String musicString){
            this.idx = idx;
            this.playTime = playTime;
            this.musicName = musicName;
            this.musicString = musicString;
        }

        //playTime desc, idx asc
        @Override
        public int compareTo(Info o){
            if(this.playTime == o.playTime){
                return this.idx - o.idx;
            }
            return o.playTime - this.playTime;
        }
    }
    static Info[] infoArr;
    static List<Info> answerList = new ArrayList<>();
    public String solution(String m, String[] musicinfos) {
        String answer = "";

        init(m.length(), musicinfos);
        for(int cnt=0; cnt<infoArr.length; cnt++){
            String musicString = infoArr[cnt].musicString;
            m=m.replaceAll("A#", "a");
            m=m.replaceAll("C#", "c");
            m=m.replaceAll("D#", "d");
            m=m.replaceAll("F#", "f");
            m=m.replaceAll("G#", "g");
            int result = musicString.indexOf(m);
            if(result != -1){
                answerList.add(infoArr[cnt]);
            }
        }

        if(answerList.size() == 0){
            return "(None)";
        }
        Collections.sort(answerList);
        return answerList.get(0).musicName;
    }

    private void init(int length, String[] musicinfos){
        infoArr = new Info[musicinfos.length];
        StringTokenizer st;
        for(int i=0; i<musicinfos.length; i++){
            st = new StringTokenizer(musicinfos[i], ",");
            infoArr[i] = new Info(
                i,
                calTime(st.nextToken(), st.nextToken()),
                st.nextToken(), st.nextToken()          //제목, 악보
            );
            ///#코드 변경
            String musicString = infoArr[i].musicString;
            musicString = musicString.replaceAll("A#", "a");
            musicString = musicString.replaceAll("C#", "c");
            musicString = musicString.replaceAll("D#", "d");
            musicString = musicString.replaceAll("F#", "f");
            musicString = musicString.replaceAll("G#", "g");

            //길이 조절
            if(infoArr[i].playTime < musicString.length()){
                System.out.println(infoArr[i].playTime);
                infoArr[i].musicString = musicString.substring(0, infoArr[i].playTime);
            }else{
                int multipleCnt = (infoArr[i].playTime / musicString.length()) + 1;
                infoArr[i].musicString = musicString.repeat(multipleCnt);
            }
            System.out.println(infoArr[i].musicString + " = " +i);
        }
    }
    private int calTime(String startTime, String endTime){
        //{hour, minute}
        int[] start = Arrays.stream(startTime.split(":")).mapToInt(Integer::parseInt).toArray();
        int[] end = Arrays.stream(endTime.split(":")).mapToInt(Integer::parseInt).toArray();

        if(end[1] < start[1]){
            end[1] += 60;
            end[0] -= 1;
        }
        return (end[0] - start[0])*60 + (end[1] - start[1]);
    }
}