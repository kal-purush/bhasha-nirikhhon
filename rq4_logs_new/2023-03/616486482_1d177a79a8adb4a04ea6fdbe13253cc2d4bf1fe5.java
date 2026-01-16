package melodic.servlet;

import melodic.dal.*;
import melodic.model.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.annotation.*;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


@WebServlet("/showuserplaylist")
public class ShowUserPlaylist extends HttpServlet {
	
	protected PlayListDao playListDao;
	
	@Override
	public void init() throws ServletException {
		playListDao = PlayListDao.getInstance();
	}
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		// Map for storing messages.
        Map<String, String> messages = new HashMap<String, String>();
        req.setAttribute("messages", messages);
		
		// Retrieve and validate UserName.
        String userId = req.getParameter("userid");
        if (userId == null || userId.trim().isEmpty()) {
            messages.put("title", "Invalid userid.");
        } else {
        	messages.put("title", "PlayList for " + userId);
        }
        
        // Retrieve BlogUsers, and store in the request.
        List<PlayList> PlayLists = new ArrayList<PlayList>();
        try {
        	User user = new User(userId);
        	playLists = playListsDao.getPlayListForUser(user);
        } catch (SQLException e) {
			e.printStackTrace();
			throw new IOException(e);
        }
        req.setAttribute("playlists", playLists);
        req.getRequestDispatcher("/ShowUserPlaylist.jsp").forward(req, resp);
	}
}