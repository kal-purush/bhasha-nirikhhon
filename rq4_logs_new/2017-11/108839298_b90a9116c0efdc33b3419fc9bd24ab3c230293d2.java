package challenge2;

import java.io.IOException;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Part;
import javax.mail.Session;
import javax.mail.Store;

public class Gmail {

    Folder inbox;
    MySQL db = new MySQL();
    Connection c;
    String DB, tablename, keyword, gmailuser, gmailpass;

    public boolean connect() {
        try {
            Properties props = System.getProperties();
            props.setProperty("mail.store.protocol", "imaps");

            Session session = Session.getDefaultInstance(props, null);
            Store store = session.getStore("imaps");
            store.connect("imap.gmail.com", gmailuser, gmailpass);

            inbox = store.getFolder("Inbox");

            return true;
        } catch (MessagingException e) {
            return false;
        }
    }
    
    public void Message_Manager () {
        try {
            c = db.MySQL_Open_Connection("root", "", "");
            c = db.createDBandTable(c, DB, tablename);
            String fecha;
            connect();
            try {
                inbox.open(Folder.READ_ONLY);
                for ( int i = 1; i < inbox.getMessageCount(); i++ )
                {
                    System.out.println("==" + i + "==");
                    if ( BusquedaDeDatos( inbox.getMessage(i) ) )
                    {
                        Date dt = inbox.getMessage(i).getReceivedDate();
                        fecha = (dt.getYear()+1900) + "-" + (dt.getMonth()+1)  + "-"
                                + dt.getDate() + " " + dt.getHours() + ":"
                                + dt.getMinutes() + ":" + dt.getSeconds() ;
                        db.insertData(c, DB, tablename, fecha,
                                Arrays.toString(inbox.getMessage(i).getFrom()),
                                inbox.getMessage(i).getSubject());
                    }
                }
                inbox.close(false);
            } catch (MessagingException ex) {
                Logger.getLogger(Gmail.class.getName()).log(Level.SEVERE, null, ex);
            }
            } catch (Exception ex) {
                Logger.getLogger(Gmail.class.getName()).log(Level.SEVERE, null, ex);
            }
    }
    
    public boolean BusquedaDeDatos( Message msg ) {
        try {
            if (msg.getSubject()!=null)                 //  Throws error when subject is empty
                if (msg.getSubject().contains(keyword))
                    return true;
                
            else if (msg.isMimeType("text/*"))
                return msg.getContent().toString().contains(keyword);
                    
            else if (msg.isMimeType("multipart/*")) {
                Multipart multi = (Multipart)msg.getContent();
                for (int j = 0; j < multi.getCount(); j++) 
                {
                    Part unaParte = multi.getBodyPart(j);
                    return unaParte.getContent().toString().contains(keyword);
                }
            }
            
        } catch (MessagingException | IOException ex) {
            Logger.getLogger(Gmail.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return false;
    }
    
    public int getAllMessageCount() {
        try {
            inbox.open(Folder.READ_ONLY);
            int count = inbox.getMessageCount();
            inbox.close(false);
            return count;
        } catch (MessagingException e) {
            System.out.println(e);
            return -1;
        }
    }
}