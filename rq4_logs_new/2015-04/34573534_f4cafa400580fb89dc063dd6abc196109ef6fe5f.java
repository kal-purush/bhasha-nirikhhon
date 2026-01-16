/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package controller;

import dao.UserDAO;
import dao.UserDAOImpl;
import java.util.Properties;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import model.UserBean;

/**
 *
 * @author it3530219
 */
@ManagedBean
@SessionScoped
public class LoginController {

    private String response;
    private String loginValidaton;
    private UserBean theModel;
    private int attemptCount;

    public LoginController() {
//        loginValidaton = "";
        theModel = new UserBean();
        attemptCount = 1;
    }

    /**
     * @return the response
     */
    public String getResponse() {
        //to be implemented based on what is required to display after logging in
        return response;
    }

    /**
     * @param response the response to set
     */
    public void setResponse(String response) {
        this.response = response;
    }

    /**
     * @return the loginValidaton
     */
    public String getLoginValidaton() {
        return loginValidaton;
    }

    /**
     * @param loginValidaton the loginValidaton to set
     */
    public void setLoginValidaton(String loginValidaton) {
        this.loginValidaton = loginValidaton;
    }

    /**
     * @return the theModel
     */
    public UserBean getTheModel() {
        return theModel;
    }

    /**
     * @param theModel the theModel to set
     */
    public void setTheModel(UserBean theModel) {
        this.theModel = theModel;
    }

    public String authenticateUser() {
        String validationMessage = null;
        System.out.println("in authenticateUser()");
        String uid = theModel.getUserName();
        String pwd = theModel.getPassword();

        System.out.println("uid=" + uid);
        System.out.println("pwd=" + pwd);

        if (uid.length() == 0) {

            loginValidaton = "Please enter your ULID to login";
            System.out.println("in authenticateUser()1: loginValidaton=" + loginValidaton);
        } else if (pwd.length() == 0) {

            loginValidaton = "Please enter your password.";
            System.out.println("in authenticateUser()2: loginValidaton=" + loginValidaton);
        } else {
            loginValidaton = "";
            System.out.println("in authenticateUser()3: loginValidaton=" + loginValidaton);
            validationMessage = findAccount();
        }
        return validationMessage;
    }

    public String findAccount() {
        UserDAO aLoginDAO = new UserDAOImpl();
        if (attemptCount < 3) {
            int rowCount = aLoginDAO.findAccount(theModel); // Doing anything with the object after this?
            if (rowCount >= 1) //a user with approved account found
            {
                theModel.setIsLoggedIn("LoggedIn");
                attemptCount = 0;
                String accountType = aLoginDAO.findUserAccountType(theModel);
                System.out.println("Account type returned =" + accountType);
                if (accountType.equals("admin")) //the user is an admin
                {
                    return "adminLandingPage.xhtml";
                } else //user is a student
                {
                    return "studentLandingPage.xhtml";
                }

            } else //a user may be either in pending status or gave incorrect username/pwd
            {
                theModel.setIsLoggedIn("NotLoggedIn");
                int pendingRowCount = aLoginDAO.findPendingAccount(theModel);
                int deniedRowCount = aLoginDAO.findDeniedAccount(theModel);
                if (pendingRowCount >= 1) //the user account in pending status
                {
                    loginValidaton = "Your account request is not yet approved. Please wait or contact the admin.";
                } else if (deniedRowCount >= 1) //the user account in denied status
                {
                    loginValidaton = "Your account request has been denied. Please contact the  admin.";
                } else//incorrect ulid/pwd
                {
                    attemptCount++;
                    loginValidaton = "The username and/or password entered is incorrect. Please try again.";
                }
                return "";
            }
        } else //user exceeded unsuccessful login attempts
        {
            loginValidaton = "You have exceeded your limit of 3 unsuccessful login attempts. "
                    + "Your account has been temporarily locked.";
            return logout();
        }
    }

    public String logout() {
//        loggedIn = false;
        theModel.setIsLoggedIn("NotLoggedIn");
        theModel.setUserName("");
        theModel.setPassword("");
        return "login.xhtml";
    }

    public String recoverPassword() {
        String ulid = theModel.getUserName();

        UserDAO aRecovery = new UserDAOImpl();
        int rowCount = aRecovery.checkUserName(ulid);
        if (rowCount <= 0) {
            loginValidaton = "This ULID is not yet registered in our system. \nPlease sign-up for an account.";
            return "";
        } else {
            loginValidaton = "A password recovery email has been sent to your ISU email address. "
                    + "Please follow the instructions provided";
            String recoveredPassword = aRecovery.retrievePassword(ulid);
            sendPasswordRecoveryEmail(ulid, recoveredPassword);
            return "login.xhtml";
        }

    }

    public void sendPasswordRecoveryEmail(String uid, String pwd) {
        // Recipient's email ID needs to be mentioned.
        String to = theModel.getEmail();
        // Sender's email ID needs to be mentioned
        String from = "msabu@ilstu.edu";
        // Assuming you are sending email from this host
        String host = "smtp.ilstu.edu";
        // Get system properties
        Properties properties = System.getProperties();
        // Setup mail server
        properties.setProperty("mail.smtp.host", host);
        properties.setProperty("mail.user", "yourID"); // if needed
        properties.setProperty("mail.password", "yourPassword"); // if needed
        // Get the default Session object.
        Session session = Session.getDefaultInstance(properties);
        try {
            // Create a default MimeMessage object.
            MimeMessage message = new MimeMessage(session);
            // Set From: header field of the header.
            message.setFrom(new InternetAddress(from));
            // Set To: header field of the header.
            message.addRecipient(Message.RecipientType.TO,
                    new InternetAddress(to));
            // Set Subject: header field
            message.setSubject("Password Recovery");
            String messageBody = "Hi,<br><br>We have received your password recovery request. "
                    + "This email contains the information that you need to log back into your account<br><br>"
                    + "User Name: " + uid + "<br>"
                    + "Password: " + pwd + "<br><br>"
                    + "Please go back to the login page and try to login with the above credentials. <br>"
                    + "If you are still unable to access your account, please call 309-309-3099 or email us at xxxx@something.com <br>"
                    + "<br><br>Best Regards,<br>Tech Support Team<br>";
            // Send the actual HTML message, as big as you like
            message.setContent(messageBody,
                    "text/html");
            // Send message
            Transport.send(message);
            System.out.println("Sent message successfully....");
        } catch (MessagingException mex) {
            mex.printStackTrace();
        }

    }

}