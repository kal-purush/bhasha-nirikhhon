package com.tereshkoff.passwordmanager.mail;

import android.app.ProgressDialog;
import android.content.Context;
import android.os.AsyncTask;
import android.widget.Toast;

import java.util.Properties;

import javax.mail.Address;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class RetreiveMailTask extends AsyncTask<String, Void, String> {

    Session session = null;
    ProgressDialog pdialog = null;
    Context context = null;
    String rec, subject, textMessage;

    public RetreiveMailTask(Session session, ProgressDialog pdialog, Context context, String rec, String subject, String textMessage)
    {
        this.session = session;
        this.pdialog = pdialog;
        this.context = context;
        this.rec = rec;
        this.subject = subject;
        this.textMessage = textMessage;
    }

    @Override
    protected String doInBackground(String... params) {

        try
        {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress("testfrom354@gmail.com"));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(rec));
            message.setSubject(subject);
            message.setContent(textMessage, "text/html; charset=utf-8");
            Transport.send(message);
        } catch(MessagingException e) {
            e.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected void onPostExecute(String result) {
        //pdialog.dismiss();
        Toast.makeText(context, "Message sent", Toast.LENGTH_LONG).show();
    }
}