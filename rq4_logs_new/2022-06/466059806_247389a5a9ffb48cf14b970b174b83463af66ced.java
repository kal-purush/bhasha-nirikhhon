package kr.ac.kpu.block.smared;

import java.util.List;

public class ChatRoom extends DTO {
    private String chatRoomName;

    private List<String> chatsUid;
    private List<String> membersUid;

    public String getChatRoomName() {
        return chatRoomName;
    }
    public void setChatRoomName(String chatRoomName) {
        this.chatRoomName = chatRoomName;
    }

    public List<String> getMembersUid() {
        return membersUid;
    }
    public void setMembersUid(List<String> membersUid) {
        this.membersUid = membersUid;
    }

    public List<String> getChatsUid() {
        return chatsUid;
    }
    public void setChatsUid(List<String> chatsUid) {
        this.chatsUid = chatsUid;
    }
}