package dataStructure.p25192;

import dataStructure.DataStructure;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

public class Main {
    public static void main(String[] args) throws IOException {
        System.setIn(new FileInputStream(DataStructure.PATH + "/p25192/data/3.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int inputCount = Integer.parseInt(br.readLine());

        ChatRoom chatRoom = new ChatRoom();
        int answer = 0;
        for (int i = 0; i < inputCount; i++) {
            String line = br.readLine();
            if ("ENTER".equals(line)) {
                answer += chatRoom.userCount();
                chatRoom.clear();
                continue; // 채팅방 다시 만들기
            }
            chatRoom.add(line);
        }
        answer += chatRoom.userCount();
        System.out.print(answer);
    }
}

class ChatRoom {

    private final HashSet<String> chatRoom;

    public ChatRoom() {
        this.chatRoom = new HashSet<>();
    }

    public void clear() {
        chatRoom.clear();
    }

    public int userCount() {
        return chatRoom.size();
    }

    public void add(String userName) {
        chatRoom.add(userName);
    }
}