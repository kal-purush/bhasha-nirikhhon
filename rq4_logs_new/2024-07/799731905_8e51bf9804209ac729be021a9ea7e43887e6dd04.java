package dataStructure.p26069;

import dataStructure.DataStructure;

import java.io.*;
import java.util.HashSet;
import java.util.StringTokenizer;

public class Main {
    public static void main(String[] args) throws NumberFormatException, IOException {
        System.setIn(new FileInputStream(DataStructure.PATH + "/p26069/data/3.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        ChongChong cc = new ChongChong();
        int count = Integer.parseInt(br.readLine());

        for (int i = 0; i < count; ++i)
            cc.letsdance(br.readLine()); // 들여쓰기

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        bw.write(String.valueOf(cc.people()));
        bw.flush();
        bw.close();
        br.close();
    }
}

class ChongChong {

    private StringTokenizer st;
    private HashSet<String> userName = new HashSet<>();

    public ChongChong(){
        userName.add("ChongChong");
    }

    public int people() {
        return userName.size();
    }

    // 로직은 여기
    public void letsdance(String userA, String userB) {
        if ("ChongChong".equals(userA) || userName.contains(userA))
            userName.add(userB);
        else if("ChongChong".equals(userB) || userName.contains(userB))
            userName.add(userA);
    }

    // 다형성(데이터 변환만)
    public void letsdance(String line) {
        st = new StringTokenizer(line);
        String userA = st.nextToken();
        String userB = st.nextToken();
        letsdance(userA, userB);
    }
}