package vn.edu.usth.outlook;

import android.widget.ImageView;

public class Email_receiver {
    String sender;
    String subject;
    String content;
    String receiver;




    ImageView profile;


    public Email_receiver(String sender,String receiver ,String subject, String content) {
        this.sender = sender;
        this.subject = subject;
        this.content = content;
        this.receiver = receiver;

    }


    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getReceiver() {return receiver;}

    public void setReceiver(String receiver) {this.receiver = receiver;}


}