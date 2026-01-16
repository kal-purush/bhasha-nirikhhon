package melodic.servlet;

import melodic.dal.PlayListDao;
import melodic.dal.TrackDao;
import melodic.dal.UserDao;
import melodic.model.*;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@WebServlet("/UserPlaylistDeleteTrack")
public class UserPlaylistDeleteTrackServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // Map for storing messages.
        Map<String, String> messages = new HashMap<String, String>();
        request.setAttribute("messages", messages);
        // Provide a title and render the JSP.
        messages.put("title", "Delete Track");
        request.getRequestDispatcher("/UserPlaylistDeleteTrack.jsp").forward(request, response);

    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        UserDao userDao = UserDao.getInstance();
        PlayListDao  playListDao = PlayListDao.getInstance();
        TrackDao trackDao = TrackDao.getInstance();
        // Map for storing messages.
        Map<String, String> messages = new HashMap<String, String>();
        request.setAttribute("messages", messages);
        boolean isDeleted = false;
        // Retrieve and validate userId.
        int userId = Integer.parseInt(request.getParameter("userid"));
        if (userId < 0) {
            messages.put("title", "Invalid userId");
            messages.put("disableSubmit", "true");
        } else {
            try {
                User user = userDao.getUserFromUserID(userId);
                List<PlayList> playLists = playListDao.getPLayListForUser(user);
                for(PlayList playList : playLists) {
                    if(playList.getPlayListId() == Long.parseLong(request.getParameter("playListId"))) {
                        List<Track> tracks = playListDao.getTracksForPLayList(playList);
                        for(Track track : tracks) {
                            if(track.getTrackId() == Long.parseLong(request.getParameter("trackId"))) {
                                trackDao.deleteTrack(track);
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }



        }

        request.getRequestDispatcher("/UserPlaylistDeleteTrack.jsp").forward(request, response);

    }
}