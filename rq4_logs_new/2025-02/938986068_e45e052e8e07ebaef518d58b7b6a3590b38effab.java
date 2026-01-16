package com.example.demo;

public class ChatMessage {
    private String senderName;
    private String message;
    private String status;
    
    public ChatMessage() {
        this.senderName = "";
        this.message = "";
        this.status = "";
    }

    public String getSenderName(){
        return this.senderName;
    }

    public void setSenderName(String senderName){
        this.senderName = senderName;
    }

    public String getMessage(){
        return this.message;
    }

    public void setMessage(String message){
        this.message = message;
    }

    public String getStatus(){
        return this.status;
    }

    public void setStatus(String status){
        this.status = status;
    }

}