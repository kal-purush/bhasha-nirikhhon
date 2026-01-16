/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/JSP_Servlet/Servlet.java to edit this template
 */
package Controllers;

import DAOs.AccountDAO;
import DAOs.CustomerDAO;
import DAOs.StaffDAO;
import Models.Account;
import Models.Customer;
import Models.Staff;
import java.io.IOException;
import java.io.PrintWriter;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 *
 * @author Dell
 */
public class ResetPasswordController extends HttpServlet {

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");
        try ( PrintWriter out = response.getWriter()) {
            /* TODO output your page here. You may use following sample code. */
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet ResetPasswordController</title>");
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet ResetPasswordController at " + request.getContextPath() + "</h1>");
            out.println("</body>");
            out.println("</html>");
        }
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String path = request.getRequestURI();
        if (path.endsWith("/ResetPasswordController")) {
            request.getRequestDispatcher("/forgotPassword.jsp").forward(request, response);
        } else {
            if (path.endsWith("/ResetPasswordController/EnterOTP")) {
                request.getRequestDispatcher("/EnterOTP.jsp").forward(request, response);
            } else {
                if (path.endsWith("/ResetPasswordController/EnterOTP/ResetPass")) {
                    request.getRequestDispatcher("/ResetPassword.jsp").forward(request, response);

                }
            }
        }
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
       if (request.getParameter("btnForgotPass") != null) {
            //get email
            String email = request.getParameter("email");

            HttpSession session = request.getSession();
            AccountDAO cdao = new AccountDAO();
            //get list email to check account of this email is already exist 
            LinkedList<String> listEmail = cdao.getAllEmail();
            if (listEmail != null) {
                boolean checkEmailExist = false;
                for (String s : listEmail) {
                    if (s.equals(email)) {
                        String check = s;
                        checkEmailExist = true;
                        break;
                    }
                }
                //if email exist, send code reset password
                if (checkEmailExist == true) {
                    session.setAttribute("email", email);

                    int otp = -1;
                    Random rand = new Random();
                    otp = 100000 + rand.nextInt(900000);
                    int rs = cdao.updateOTP(otp + "", email);
                    //send code to email
                    final String serverEmail = "lovecake.noreply@gmail.com";
                    final String serverEmailPass = "fcqxvwldkcnmvtoy";

                    Properties prop = new Properties();
                    prop.put("mail.smtp.host", "smtp.gmail.com");//smtp host
                    prop.put("mail.smtp.port", "587");//tls 587 and ssl 465
                    prop.put("mail.smtp.auth", "true");
                    prop.put("mail.smtp.starttls.enable", "true");
                    //create authenticator
                    Authenticator auth = new Authenticator() {
                        protected PasswordAuthentication getPasswordAuthentication() {
                            return new PasswordAuthentication(serverEmail, serverEmailPass);
                        }
                    };

                    //create session
                    Session sessionEmail = Session.getInstance(prop, auth);
                    //send email
                    String sendTo = email;
                    //create compose message email
                    MimeMessage msg = new MimeMessage(sessionEmail);
                    try {

                        msg.addHeader("Content-type", "text/html;charset=UTF-8");
                        msg.setFrom(new InternetAddress(serverEmail));
//                            InternetAddress[] recipientAddress = InternetAddress.parse(sendTo, false);

                        msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(sendTo, false));
                        //tieu de email
                        msg.setSubject("Ma OTP");
                        msg.setText("your OTP is: " + otp);
                        Transport.send(msg);

                    } catch (MessagingException ex) {
                        Logger.getLogger(ResetPasswordController.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    response.sendRedirect("/ResetPasswordController/EnterOTP");

                } else {
                    session.setAttribute("messageErrorEmail", "not find");
                    response.sendRedirect("/ResetPasswordController");

                }
            }

        }

        if (request.getParameter("btnOTP") != null) {
            String otp = request.getParameter("otp");
            if (otp != null || !otp.equals("")) {
                AccountDAO cdao = new AccountDAO();
                HttpSession session = request.getSession();
                String email = session.getAttribute("email").toString();
                int getOtp = cdao.getResetCodeByEmail(email);

                if (otp.equals(getOtp + "")) {
                    response.sendRedirect("/ResetPasswordController/EnterOTP/ResetPass");
                } else {
                    response.sendRedirect("/ResetPasswordController/EnterOTP");

                }

            }

        }
        if (request.getParameter("btnReset") != null) {
            //get password in form
            String password = request.getParameter("password");
            //create dao
            AccountDAO cdao = new AccountDAO();
            CustomerDAO cusdao = new CustomerDAO();
            StaffDAO staffdao = new StaffDAO();

            HttpSession session = request.getSession();
            String email = session.getAttribute("email").toString();
            Account acc = cdao.getAccByEmail(email);
            int editPassInAccount = cdao.editPassword(password, email);
            Customer cus = cusdao.getCustomer(acc.getAcc_id());
            Staff staff = staffdao.getStaff(acc.getAcc_id());
            if (cus != null) {
                int editCus = cusdao.editPassword(password, email);
                response.sendRedirect("/LoginController");
            } else if (staff != null) {
                int editStaff = staffdao.editPassword(password, email);
                response.sendRedirect("/LoginController");

            }

        }
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}