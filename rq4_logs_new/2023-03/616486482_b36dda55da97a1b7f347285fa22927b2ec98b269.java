package melodic.servlet;

import melodic.dal.*;
import melodic.model.*;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.annotation.*;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


@WebServlet("/userupdate")
public class UserUpdateServlet extends HttpServlet {
	
	protected UserDao userDao;
	
	@Override
	public void init() throws ServletException {
		userDao = UserDao.getInstance();
	}
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		// Map for storing messages.
        Map<String, String> messages = new HashMap<String, String>();
        req.setAttribute("messages", messages);
        //Just render the JSP.   
        
        User user = null;
        
        // Retrieve and validate userId.
        String input = req.getParameter("userid");
        if (input == null ) {
            messages.put("title", "Invalid userId");
            messages.put("disableSubmit", "true");
        } else {
        	// Retrieve BlogUsers, and store as a message.
        	int userId = Integer.valueOf(input);
        	try {
        		user = userDao.getUserFromUserID(userId);
            } catch (SQLException e) {
    			e.printStackTrace();
    			throw new IOException(e);
            }
        	messages.put("success", "Displaying results for " + userId);
        	// Save the previous search term, so it can be used as the default
        	// in the input box when rendering FindUser.jsp.
        	messages.put("previousUserId", String.valueOf(userId));
        }
        req.setAttribute("melodicMindsUser", user);
        
        req.getRequestDispatcher("/UserUpdate.jsp").forward(req, resp);
	}
	
	@Override
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
    		throws ServletException, IOException {
        // Map for storing messages.
        Map<String, String> messages = new HashMap<String, String>();
        req.setAttribute("messages", messages);

        // Retrieve and validate name.
        int userId = Integer.parseInt(req.getParameter("userid"));
        
        String userName = req.getParameter("username");
        if (userName == null || userName.trim().isEmpty()) {
            messages.put("success", "Invalid UserName");
        } else {
        	// Create the StandardUser.
        	String email = req.getParameter("email");
        	// dob must be in the format yyyy-mm-dd.
        	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        	String stringDob = req.getParameter("dob");
        	String description = req.getParameter("description");
        	String address = req.getParameter("address");
        	Date dob;
        	
        	try {
        		dob = dateFormat.parse(stringDob);
        	} catch (ParseException e) {
        		e.printStackTrace();
				throw new IOException(e);
        	}
        	
	        try {
	        	User user = new User(userId, userName, email, dob, description, true, address);
	        	userDao.update(user);
	        	messages.put("success", "Successfully updated " + userName + "; userId: " + user.getUserId());
	        } catch (SQLException e) {
				e.printStackTrace();
				throw new IOException(e);
	        }
        }
        
        req.getRequestDispatcher("/FindUsers.jsp").forward(req, resp);
    }
}