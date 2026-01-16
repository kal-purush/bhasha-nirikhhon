package POP3_Retrieve_messages;

import org.jsoup.Jsoup;
import javax.mail.*;
import javax.mail.internet.MimeMultipart;
import java.util.Enumeration;
import java.util.Properties;

public class RetrieveEmails {
    public static final String USERNAME = "zaharia.gabi99@gmail.com";
    public static final String PASSWORD = "18061997Zaharia12";

    public static void main(String[] args) {
        // mail server connection parameters.
        // setting the host configuration.
        String host = "pop.gmail.com";

        //open the session and connect to the mail inbox folder.
        Properties properties = GetProperties();
        Session session = Session.getDefaultInstance(properties);
        try {
            Store store = session.getStore("pop3");
            store.connect(host, USERNAME, PASSWORD);
            Folder inbox = store.getFolder("INBOX");
            inbox.open(Folder.READ_ONLY);

            // get/retrieving the list of inbox messages
            Message[] messages = inbox.getMessages();

            if (messages.length == 0) System.out.println("No new messages were found!");

            for (int i = 0; i < messages.length; i++) {
                System.out.println(">> Message #" + (i + 1));
                System.out.println(">> From: " + messages[i].getFrom()[0]);
                System.out.println(">> Subject: " + messages[i].getSubject());
                System.out.println(">> Sent Date: " + messages[i].getSentDate());
                System.out.println(">> Message:" + getTextFromMessage(messages[i]));
                System.out.println("\n>> Headers:");
                Enumeration e = messages[i].getAllHeaders();
                while (e.hasMoreElements()) {
                    Header header = (Header) e.nextElement();
                    System.out.println(header.getName() + ": " + header.getValue());
                }
                System.out.println("\n- - - - - - - - - - - - - - - - - - - - - - - - - -");
            }

            inbox.close(true);
            store.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //configuration settings for properties
    private static Properties GetProperties(){
        Properties props = new Properties();
        props.put("mail.pop3.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.pop3.socketFactory.port", "995");
        props.put("mail.pop3.port", "995");
        props.put("mail.pop3.host", "pop.gmail.com");
        props.put("mail.pop3.user", USERNAME);
        props.put("mail.store.protocol", "pop3");
        return props;
    }
    // get the text of the message in plain mode in cosole.
    private static String getTextFromMessage(Message message) throws Exception {
        String result = "";
        if (message.isMimeType("text/plain")) {
            result = message.getContent().toString();
        } else if (message.isMimeType("multipart/*")) {
            MimeMultipart mimeMultipart = (MimeMultipart) message.getContent();
            result = getTextFromMimeMultipart(mimeMultipart);
        }
        return result;
    }
    // parsing the parts of the message (the Bodyparts)
    private static String getTextFromMimeMultipart(MimeMultipart mimeMultipart) throws Exception {
        String result = "\n";
        int count = mimeMultipart.getCount();

        for (int i = 0; i < count; i++) {
            BodyPart bodyPart = mimeMultipart.getBodyPart(i);
            if (bodyPart.isMimeType("text/plain")) {
                result += bodyPart.getContent();
                break;
            } else if (bodyPart.isMimeType("text/html")) {
                String html = (String) bodyPart.getContent();
                result += Jsoup.parse(html).text();
            } else if (bodyPart.getContent() instanceof MimeMultipart){
                result += getTextFromMimeMultipart((MimeMultipart)bodyPart.getContent());
            }
        }
        return result;
    }
}