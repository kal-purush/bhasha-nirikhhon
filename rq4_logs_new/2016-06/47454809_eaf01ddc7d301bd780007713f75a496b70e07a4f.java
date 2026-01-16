package servlet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;
import javax.persistence.Column;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.sql.DataSource;

import models.Event;
import models.Film;
import models.Friend;
import models.User;

/**
 * Servlet implementation class FriendProfileServlet
 */
@WebServlet("/FriendProfileServlet")
public class FriendProfileServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	@Resource private DataSource myDataSource;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public FriendProfileServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
	{
		/*// TODO Auto-generated method stub
		response.getWriter().append("Served at: ").append(request.getContextPath());*/
		
		
		
		try 
		{
			String friendId = request.getParameter("friendId");
	        //Setting up database connection
	        Connection myConnection = myDataSource.getConnection("APP","APP");			
	        System.out.println("i am in do GET FriendsServlet, Got Connection!");

			HttpSession session;
			session = request.getSession();
	        int id = (Integer)session.getAttribute("sessionId");
	        System.out.println("i am in do GET FriendsProfileServlet, preparing  session");
	        
	     
	        
	      //**************************************** GETTING FRIENDS INFORMATION ******************************************************
	        PreparedStatement statementFriends = myConnection.prepareStatement("SELECT * from USERS where ID_USER =" + friendId);
	        	        	      
			ResultSet newFriendsResult = statementFriends.executeQuery();
			//List<Friend> friendsList = new ArrayList<Friend>();
			System.out.println("i am in do GET FriendsProfileServlet, preparing friends list");
			if(newFriendsResult.next())
			{
				System.out.println("i am in do GET FriendsProfileServlet, executed list friends ");
				Friend friendd = new Friend();
				
				int idFriend = newFriendsResult.getInt("ID_USER");
				String nomFriend = newFriendsResult.getString("NOM_USER");
				String prenomFriend = newFriendsResult.getString("PRENOM_USER");
				String sexeFriend = newFriendsResult.getString("SEXE");
				String usernameFriend = newFriendsResult.getString("USERNAME");
				String passwordFriend = newFriendsResult.getString("PASSWORD_USER");
				String emailFriend =  newFriendsResult.getString("EMAIL_USER");
				String BdayFriend =  newFriendsResult.getString("DATE_OF_BIRTH");
				String creationDateFriend =  newFriendsResult.getString("USER_CREATION_DATE");
				String modificationDateFriend =  newFriendsResult.getString("USER_MODIFICATION_DATE");
				String addressFriend =  newFriendsResult.getString("ADDRESS_USER");
				String imageFriend =  newFriendsResult.getString("IMAGE_USER");
				
				friendd.setId(idFriend);
				friendd.setFullName(prenomFriend + " " + nomFriend);
												
				User currentUserFriend = new User(idFriend,prenomFriend,nomFriend,usernameFriend,passwordFriend,emailFriend,BdayFriend, sexeFriend, addressFriend, imageFriend, modificationDateFriend, creationDateFriend);
				
				friendd.setUserr(currentUserFriend);
				
				
				//**************************************** GETTING FRIENDS IN COMMON ******************************************************
				
				 List<User> listFriendsCommon = new ArrayList<User>();
				
				/*PreparedStatement statementFriendsInCommon = myConnection.prepareStatement("SELECT u.ID_USER, u.NOM_USER, u.PRENOM_USER, u.EMAIL_USER, u.DATE_OF_BIRTH, u.SEXE, u.USERNAME, u.PASSWORD_USER, u.ADDRESS_USER, u.IMAGE_USER, u.USER_CREATION_DATE, u.USER_MODIFICATION_DATE "
		        		+ "FROM FRIENDS f "
		        		+ "inner join USERS u on f.ID_USER_TWO = u.ID_USER "
		        		+ "WHERE (f.ID_USER_ONE = " + id +" or f.ID_USER_TWO = " + id +") and FRIENDS_STATE='Accepted'");
				
				
				 ResultSet newFriendsInCommonResult = statementFriendsInCommon.executeQuery();
					//List<Friend> friendsList = new ArrayList<Friend>();
					System.out.println("i am in do GET FriendsProfileServlet, preparing friends list");
					if(newFriendsInCommonResult.next())
					{
						
						int idFriends = newFriendsResult.getInt("ID_USER");
						String nomFriends = newFriendsResult.getString("NOM_USER");
						String prenomFriends = newFriendsResult.getString("PRENOM_USER");
						String sexeFriends = newFriendsResult.getString("SEXE");
						String usernameFriends = newFriendsResult.getString("USERNAME");
						String passwordFriends = newFriendsResult.getString("PASSWORD_USER");
						String emailFriends =  newFriendsResult.getString("EMAIL_USER");
						String BdayFriends =  newFriendsResult.getString("DATE_OF_BIRTH");
						String creationDateFriends =  newFriendsResult.getString("USER_CREATION_DATE");
						String modificationDateFriends =  newFriendsResult.getString("USER_MODIFICATION_DATE");
						String addressFriends =  newFriendsResult.getString("ADDRESS_USER");
						String imageFriends =  newFriendsResult.getString("IMAGE_USER");
						
						User currentUserFriends = new User(idFriends,prenomFriends,nomFriends,usernameFriends,passwordFriends,emailFriends,BdayFriends, sexeFriends, addressFriends, imageFriends, modificationDateFriends, creationDateFriends);
						
						
						
					}
				*/
				
				
					friendd.setListFriendsCommon(listFriendsCommon);
				//**************************************** GETTING EVENTS IN COMMON *******************************************************
				List<Event> listEventsCommon = new ArrayList<Event>();
			
				PreparedStatement statementEventsInCommon = myConnection.prepareStatement("select * from (SELECT * FROM GROUPE e "
																	+" inner join USER_APPARTIENT_GROUPE ug on ug.ID_GROUPE_APPARTIENT = e.ID_GROUPE"
																	+" inner join USERS u on ug.ID_USER_APPARTIENT = u.ID_USER"
																	+" inner join FILM_CONTIENT_GROUPE fc on fc.ID_GROUPE_CONTIENT = e.ID_GROUPE"
																	+" inner join FILM f on f.ID_FILM = fc.ID_FILM_CONTIENT"
																	+" WHERE u.ID_USER =  " + friendId +" ) as events"
																	+" WHERE events.ID_GROUPE IN "
																	+" (SELECT e.ID_GROUPE FROM GROUPE e "
																	+" inner join USER_APPARTIENT_GROUPE ug on ug.ID_GROUPE_APPARTIENT = e.ID_GROUPE"
																	+" inner join USERS u on ug.ID_USER_APPARTIENT = u.ID_USER"
																	+" WHERE u.ID_USER = " + id +")");
				
				
				ResultSet newEventsInCommonResult = statementEventsInCommon.executeQuery();
				System.out.println("i am in do GET FriendsProfileServlet, preparing friends list");
				while(newEventsInCommonResult.next())
				{
					
					int FilmId = newEventsInCommonResult.getInt("ID_FILM");
					String FilmImage = newEventsInCommonResult.getString("FILM_IMAGE");
					String FilmNom = newEventsInCommonResult.getString("NOM_FILM");
					String EventDate = newEventsInCommonResult.getString("WATCHING_DATE");
					String EventName = newEventsInCommonResult.getString("NOM_GROUPE");
					int EventId = newEventsInCommonResult.getInt("ID_GROUPE");
					
					Film newFilm = new Film(FilmId, FilmNom, FilmImage);
					
					Event newEvent = new Event(EventId,EventName,EventDate,newFilm);
										
					listEventsCommon.add(newEvent);					
				}
				
				
				
				friendd.setListEventsCommon(listEventsCommon);
				//**************************************** GETTING MOVIES HE HAS SEEN IF HE IS FRIEND *************************************							
				// >>>>>>>>>>>>>>> checking if he is a friends				
				PreparedStatement statementFriend = myConnection.prepareStatement("(SELECT * FROM FRIENDS f"   /*u.ID_USER, u.NOM_USER, u.PRENOM_USER, u.EMAIL_USER, u.DATE_OF_BIRTH, u.SEXE, u.USERNAME, u.PASSWORD_USER, u.ADDRESS_USER, u.IMAGE_USER, u.USER_CREATION_DATE, u.USER_MODIFICATION_DATE */
															+ "	inner join USERS u on f.ID_USER_TWO = u.ID_USER "
															+ "	WHERE f.ID_USER_ONE = " + id + " and f.ID_USER_TWO = "+ idFriend +")"   /*+" and f.FRIENDS_STATE='Accepted'	*/
															+ "	UNION "																	
															+ " (SELECT *  FROM FRIENDS f1"    /*u1.ID_USER, u1.NOM_USER, u1.PRENOM_USER, u1.EMAIL_USER, u1.DATE_OF_BIRTH, u1.SEXE, u1.USERNAME, u1.PASSWORD_USER, u1.ADDRESS_USER, u1.IMAGE_USER, u1.USER_CREATION_DATE, u1.USER_MODIFICATION_DATE*/
															+ " inner join USERS u1 on f1.ID_USER_ONE = u1.ID_USER "
															+ " WHERE f1.ID_USER_TWO = " + id + "  and f1.ID_USER_ONE = "+ idFriend +")");    /* and f1.FRIENDS_STATE='Accepted'*/
						
								
				ResultSet newFriendsResult2 = statementFriend.executeQuery();
				//List<Friend> friendsList = new ArrayList<Friend>();
				System.out.println("i am in do GET FriendsProfileServlet, preparing friends list");
				if(newFriendsResult2.next())
				{
					
					String FRIENDS_STATE = newFriendsResult2.getString("FRIENDS_STATE");									
					
					request.setAttribute("isFriend", FRIENDS_STATE);
				}
				else
				{
					request.setAttribute("isFriend", false);
				}
				// >>>>>>>>>>>>>>> getting his movie list
				
				List<Film> ListFriendsMovies = new ArrayList<Film>();
				PreparedStatement statementFriendsMovies = myConnection.prepareStatement("select * from (SELECT * FROM GROUPE e "
						+" inner join USER_APPARTIENT_GROUPE ug on ug.ID_GROUPE_APPARTIENT = e.ID_GROUPE"
						+" inner join USERS u on ug.ID_USER_APPARTIENT = u.ID_USER"
						+" inner join FILM_CONTIENT_GROUPE fc on fc.ID_GROUPE_CONTIENT = e.ID_GROUPE"
						+" inner join FILM f on f.ID_FILM = fc.ID_FILM_CONTIENT"
						+" WHERE u.ID_USER =  " + friendId +" ) as events");
				
				ResultSet newFriendsMovies = statementFriendsMovies.executeQuery();
				System.out.println("i am in do GET FriendsProfileServlet, preparing friends list");
				while(newFriendsMovies.next())
				{					
					int FilmId = newFriendsMovies.getInt("ID_FILM");
					String FilmImage = newFriendsMovies.getString("FILM_IMAGE");
					String FilmNom = newFriendsMovies.getString("NOM_FILM");
					int FilmRating = newFriendsMovies.getInt("NOTATION_FILM");					
					
					Film newFilm = new Film(FilmId, FilmNom, FilmImage, FilmRating);													
					ListFriendsMovies.add(newFilm);			
				}
				
				friendd.setListMovies(ListFriendsMovies);
				//**************************************** SETTING friend value in request *************************************													
				request.setAttribute("friend", friendd);
			}									
						  								
			System.out.println("redirecting to friends page");
			request.getRequestDispatcher("/AddFriend.jsp").forward(request, response);
			myConnection.close();
			System.out.println("i am out of do GET FriendsServlet");
		} 		
		catch (SQLException e) 
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
	{
			
	}
}