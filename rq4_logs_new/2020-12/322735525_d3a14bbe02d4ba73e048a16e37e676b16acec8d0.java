import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Scanner;

public class MovieReservation {
	 static final String DB_URL = "jdbc:mysql://localhost:3306/movie_reservation?serverTimezone=UTC";
	 static final String USER = "root";
	 static final String PASS = "";
	 
	 public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		Scanner input = new Scanner(System.in);
		
		try {
			System.out.println("Connecting...\n");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			
			System.out.println("Welcome to the Movie Reservation System!");
			System.out.println("=========================================");
		    
			String accountChoice = "";
			while (!accountChoice.equals("LOGIN") && !accountChoice.equals("CREATE ACCOUNT") && !accountChoice.equals("CREATE")) {
				System.out.print("LOGIN or CREATE ACCOUNT? ");
				accountChoice = input.nextLine().toUpperCase();
				System.out.println();
			}
			
			String username = "";
			String password = "";
			String accountType = "";
			
			if (accountChoice.equals("CREATE ACCOUNT") || accountChoice.equals("CREATE")) {
				String roleChoice = "";
				while (!roleChoice.equals("user") && !roleChoice.equals("admin")) {
					System.out.print("Create a USER or ADMIN? ");
					roleChoice = input.nextLine().toLowerCase();
					System.out.println();
				}
				
				System.out.println("CREATE YOUR ACCOUNT");
				System.out.print("Enter new username: ");
				username = input.nextLine();
				System.out.print("Enter new password of greater than 5 characters: ");
				password = input.nextLine();
				if (password.length() < 5) {
					System.out.println("\nPassword automatically changed to 'password' due to password being fewer than 5 character.\n");
				}
				stmt.executeUpdate("INSERT INTO Users VALUES ('" + username + "', '" + password + "', '" + roleChoice + "')");
				System.out.println(roleChoice.toUpperCase() + " account successfully created! Please login...\n");
			}
			
			while (true) {
				System.out.println("LOGIN");
				System.out.print("Enter username: ");
				username = input.nextLine();
				System.out.print("Enter password: ");
				password = input.nextLine();

				rs = stmt.executeQuery("SELECT * FROM Users WHERE username = '" + username + "' AND password = '"
						+ password + "'");
				if (rs.next()) {
					System.out.println("You are logged in as " + rs.getString("accountType").toUpperCase() + "! Welcome, " 
								+ rs.getString("username") + "\n");
					accountType = rs.getString("accountType");
					break;
				} 
				else
					System.out.println("No account exists... Please try again.\n");
			}
			
			System.out.println("=========================================");
			int operation = -1;
			if (accountType.equals("user")) {
				while (operation != 0) {
					System.out.print("[1] Reservations\n[2] Search movies\n[3] View movies\n[4] Search people\n"
							+ "[0] Logout\nEnter a number: ");
					operation = input.nextInt();
					input.nextLine();
					System.out.println();
					switch (operation) { 
						case 1:
							operation = -1;
							while (operation != 0) {
								System.out.print(" - Reservations Menu -\n[1] Make reservation\n[2] Cancel reservation\n[3] "
										+ "View reservations\n" + "[4] View showtimes\n[0] Return\nEnter a number: ");
								operation = input.nextInt();
								input.nextLine();
								System.out.println();
								switch (operation) {
									case 1:
										rs = stmt.executeQuery("SELECT * FROM ShowTimes, Movies WHERE Showtimes.mID = Movies.mID");
										if (rs.next() == true) {
											System.out.println("Showtime ID: " + rs.getString("sID") + " | Movie: " 
													+ rs.getString("title") + " | Showtime: " + rs.getString("startTime"));
											while (rs.next()) {
												System.out.println("Showtime ID: " + rs.getString("sID") + " | Movie: " 
														+ rs.getString("title") + " | Showtime: " + rs.getString("startTime"));
											}
											System.out.print("Enter an ID: ");
											int sID = input.nextInt();
											input.nextLine();
											stmt.executeUpdate("INSERT INTO RESERVATIONS(username, sID) VALUES ('" + username + "', " 
													+ sID + ")");
											System.out.println("Reservation successfully made!\n");
										}else {
											System.out.println("No showtimes available for reservation!");
										}
										break;
									case 2:
										System.out.println(username + "'s Reservations");
										rs = stmt.executeQuery("SELECT rID, title, startTime, creationTime FROM Reservations, Showtimes,"
												+ " Movies WHERE Reservations.sID = Showtimes.sID AND Showtimes.mID = Movies.mID "
												+ "AND username = '" + username + "'");
										while (rs.next()) {
											System.out.println("Reservation ID: " + rs.getInt(1) + " | Movie: " + rs.getString(2) + 
													" | Showtime: " + rs.getString(3) + " | Timestamp: " + rs.getTimestamp(4));
										}
										System.out.print("\nEnter a Reservation ID: ");
										int rID = input.nextInt();
										input.nextLine();
										stmt.executeUpdate("DELETE FROM RESERVATIONS WHERE rID = " + rID);
										System.out.println("Reservation successfully cancelled!\n");
										break;
									case 3:
										System.out.println(username + "'s Reservations");
										rs = stmt.executeQuery("SELECT rID, title, startTime, creationTime FROM Reservations, Showtimes,"
												+ " Movies WHERE Reservations.sID = Showtimes.sID AND Showtimes.mID = Movies.mID "
												+ "AND username = '" + username + "'");
										while (rs.next()) {
											System.out.println("Reservation ID: " + rs.getInt(1) + " | Movie: " + rs.getString(2) + 
													" | Showtime: " + rs.getString(3) + " | Timestamp: " + rs.getTimestamp(4));
										}
										System.out.println("");
										break;
									case 4:
										System.out.println("Showtimes");
										rs = stmt.executeQuery("SELECT title, startTime FROM Movies LEFT OUTER JOIN Showtimes ON "
												+ "Movies.mID = Showtimes.mID");
										while (rs.next()) {
											if (rs.getTime(2) == null)
												System.out.println("Movie: " + rs.getString(1) + " | Showtime: N/A");
											else
												System.out.println("Movie: " + rs.getString(1) + " | Showtime: " + rs.getTime(2));
										}
										System.out.println("");
										break;
									default:
										break;
								}
							}
							operation = -1;
							break;
						case 2:
							operation = -1;
							while (operation != 0) {
								System.out.print(" - Search Movies Menu -\n[1] Search by title\n[2] Search by year\n[3] Search by genre\n"
										+ "[4] Search by 2 genres\n[5] Search by people\n[6] Search by IMDb score\n[0] Return\nEnter "
										+ "a number: ");
								operation = input.nextInt();
								input.nextLine();
								System.out.println("");
								switch (operation) {
									case 1:
										System.out.print("Enter title: ");
										String title = input.nextLine();
										rs = stmt.executeQuery("SELECT title, releaseYear, runTime, genre FROM Movies WHERE title LIKE '%" 
												+ title + "%'");
										while (rs.next()) {
											System.out.println("Movie: " + rs.getString(1) + " | Year: " + rs.getInt(2) + " | Runtime: "
													+ rs.getInt(3) + " min | Genre: " + rs.getString(4));
										}
										System.out.println("");
										break;
									case 2:
										System.out.print("Enter year: ");
										int year = input.nextInt();
										input.nextLine();
										rs = stmt.executeQuery(" SELECT title, releaseYear, runTime, genre FROM Movies WHERE releaseYear" 
												+ " = " + year);
										while (rs.next()) {
											System.out.println("Movie: " + rs.getString(1) + " | Year: " + rs.getInt(2) + " | Runtime: "
													+ rs.getInt(3) + " min | Genre: " + rs.getString(4));
										}
										System.out.println("");
										break;
									case 3:
										System.out.print("Enter genre: ");
										String genre = input.nextLine();
										rs = stmt.executeQuery("SELECT title, releaseYear, runTime, genre FROM Movies WHERE genre LIKE '%" 
												+ genre + "%'");
										while (rs.next()) {
											System.out.println("Movie: " + rs.getString(1) + " | Year: " + rs.getInt(2) + " | Runtime: "
													+ rs.getInt(3) + " min | Genre: " + rs.getString(4));
										}
										System.out.println("");
										break;
									case 4:
										System.out.print("Enter genre: ");
										String genre1 = input.nextLine();
										System.out.print("Enter second genre: ");
										String genre2 = input.nextLine();
										rs = stmt.executeQuery("SELECT title, releaseYear, runTime, genre FROM Movies where genre LIKE '%" 
												+ genre1 + "%' UNION" + " SELECT title, releaseYear, runTime, genre FROM Movies where "
												+ "genre LIKE '%" + genre2 + "%'");
										while (rs.next()) {
											System.out.println("Movie: " + rs.getString(1) + " | Year: " + rs.getInt(2) + " | Runtime: "
													+ rs.getInt(3) + " min | Genre: " + rs.getString(4));
										}
										System.out.println("");
										break;
									case 5:
										System.out.print("Enter a cast member: ");
										String person = input.nextLine();
										rs = stmt.executeQuery("SELECT pID FROM People WHERE name LIKE '%" + person + "%'");
										ArrayList<String> pIDs = new ArrayList<>();
										while (rs.next())
											pIDs.add(rs.getString(1));
										if (!pIDs.isEmpty()) {
											for (String pID: pIDs) {
												rs = stmt.executeQuery("SELECT title, releaseYear, runTime, genre, name, "
														+ "People.profession FROM Movies, Cast, People WHERE Cast.mID = Movies.mID "
														+ "AND Cast.pID = People.pID AND People.PID = '" + pID + "'");
												while (rs.next()) {
													System.out.println("Movie: " + rs.getString(1) + " | Year: " + rs.getInt(2) 
															+ " | Runtime: " + rs.getInt(3) + " min | Genre: " + rs.getString(4) 
															+ " | Cast Member: " + rs.getString(5) + " | Role: " + rs.getString(6));
												}
											}
										}
										System.out.println("");
										break;
									case 6:
										System.out.print("Score should not be less than (MINIMUM: 0): ");
										float score1 = input.nextFloat();
										System.out.print("Score should not be greater than (MAXIMUM: 10): ");
										float score2 = input.nextFloat();
										input.nextLine();
										rs = stmt.executeQuery("SELECT title, releaseYear, runTime, genre, imdbScore FROM Movies, Ratings"
												+ " WHERE Movies.mID = Ratings.mID AND imdbScore >= " + score1 + "AND imdbScore <= " 
												+ score2);
										while (rs.next()) {
											System.out.println("Movie: " + rs.getString(1) + " | Year: " + rs.getInt(2) + " | Runtime: "
													+ rs.getInt(3) + " min | Genre: " + rs.getString(4) + " | IMDb Score: " 
													+ rs.getFloat(5));
										}
										System.out.println("");
										break;
									default:
										break;
								}
							}
							operation = -1;
							break;
						case 3:
							operation = -1;
							while (operation != 0) {
								System.out.print(" - View Movies Menu -\n[1] View all movies\n[2] View all movie ratings\n[3] View cast "
										+ "of a movie\n[4] View highest rated genre\n[5] View average rating of each year\n[6] View "
										+ "highest rated movie\n[7] View lowest rated movie\n[0] Return\nEnter a number: ");
								operation = input.nextInt();
								input.nextLine();
								System.out.println("");
								switch (operation) {
									case 1:
										rs = stmt.executeQuery("SELECT * FROM Movies");
										while (rs.next()) {
											System.out.print("Movie: " + rs.getString(2) + " | Year: " + rs.getInt(3) + " | Runtime: " 
													+ rs.getInt(4) + " min | Genre: " + rs.getString(5) + "\n");
										}
										System.out.println();
										break;
									case 2:
										rs = stmt.executeQuery("SELECT title, releaseYear, imdbScore FROM Movies, Ratings "
												+ "WHERE Movies.mID = Ratings.mID");
										while (rs.next()) {
											System.out.print("Movie: " + rs.getString(1) + " | Year: " + rs.getInt(2) + " | IMDb Score: " 
													+ rs.getFloat(3) + "\n");
										}
										System.out.println();
										break;
									case 3:
										System.out.print("Enter a movie: ");
										String title = input.nextLine();
										rs = stmt.executeQuery("SELECT title FROM Movies WHERE title LIKE '%" + title + "%'");
										ArrayList<String> movies = new ArrayList<>();
										while (rs.next())
											movies.add(rs.getString(1));
										for (String movie: movies) {
											rs = stmt.executeQuery("SELECT name FROM Movies, Cast, People WHERE Movies.mID = Cast.mID AND"
													+ " Cast.pID = People.pID AND title = '" + movie + "'");
											System.out.println("\nCast of " + movie +"\n----------");
											while (rs.next()) {
												System.out.println(rs.getString(1));
											}
											System.out.println("----------");
										}
										System.out.println("");
										break;
									case 4:
										System.out.println("");
										rs = stmt.executeQuery("SELECT genre, AVG(imdbScore) FROM Movies, Ratings WHERE Movies.mID = "
												+ "Ratings.mID GROUP BY genre HAVING AVG(imdbScore) >= ALL(SELECT AVG(imdbScore) FROM "
												+ "Movies, Ratings WHERE Movies.mID = Ratings.mID GROUP BY genre)");
										if (rs.next()) {
											System.out.print("Genre: " + rs.getString(1) + " | Average IMDb Score: " + rs.getFloat(2) 
												+ "\n");
										}
										System.out.println();
										break;
									case 5:
										rs = stmt.executeQuery("SELECT DISTINCT(releaseYear) FROM Movies ORDER BY releaseYear DESC;");
										ArrayList<String> years = new ArrayList<String>();
										while (rs.next()) {
											years.add(rs.getString(1));											
										}
										rs = stmt.executeQuery("SELECT releaseYear AS Year, AVG(imdbScore) AS Average_Rating FROM Movies,"
												+ " Ratings WHERE Movies.mID = Ratings.mID GROUP BY releaseYear ORDER BY Average_Rating "
												+ "DESC;");
										System.out.println("Years sorted by highest average rating:\n");
										int j = 1;
										while (rs.next()) {
											System.out.println("[Rank: " + (j) + "] Year: " + rs.getString(1) + ", Average Rating: " 
													+ rs.getString(2).substring(0,4));
											j++;
										}
										System.out.println();
										break;
									case 6:
										System.out.println("\nThe Highest Rated Movie(s) of any genre/year was:\n");
										rs = stmt.executeQuery("SELECT title, releaseYear, runTime, genre, imdbScore FROM Movies JOIN "
												+ "Ratings R1 USING (mID) WHERE NOT EXISTS (SELECT * FROM Movies, Ratings R2 WHERE R2.mID"
												+ " = Movies.mID AND R1.imdbScore < R2.imdbScore);");
										while (rs.next()) {
											System.out.println("Title: " + rs.getString(1));
											System.out.println("Year: " + rs.getString(2));
											System.out.println("Run Time: " + rs.getString(3));
											System.out.println("Genre: " + rs.getString(4));
											System.out.println("Rating: " + rs.getString(5));
										}
										System.out.println();
										break;
									case 7:
										System.out.println("\nThe Lowest Rated Movie(s) of any genre/year was:");
										rs = stmt.executeQuery("SELECT title, releaseYear, runTime, genre, imdbScore FROM Movies JOIN "
												+ "Ratings R1 USING (mID) WHERE NOT EXISTS (SELECT * FROM Movies, Ratings R2 WHERE "
												+ "R2.mID = Movies.mID AND R1.imdbScore > R2.imdbScore);");
										while (rs.next()) {
											System.out.println("Title: " + rs.getString(1));
											System.out.println("Year: " + rs.getString(2));
											System.out.println("Run Time: " + rs.getString(3));
											System.out.println("Genre: " + rs.getString(4));
											System.out.println("Rating: " + rs.getString(5));
										}
										System.out.println();
										break;
									default:
										break;
								}
							}
							operation = -1;
							break;
						case 4:
							operation = -1;
							while (operation != 0) {
								System.out.print(" - Search People Menu -\n[1] Search actors\n[2] Search directors\n"
										+ "[3] Search producers\n[4] Find all movies a person is involved in\n[5] Find people who were only "
										+ "involved in movies from X year\n[0] Return\nEnter a number: ");
								operation = input.nextInt();
								input.nextLine();
								System.out.println("");
								switch (operation) {
									case 1:
										System.out.print("Enter a name to search for or press ENTER to show all actors: ");
										String nameToSearch = input.nextLine();
										System.out.println();
										if (nameToSearch.equals("")) {
											rs = stmt.executeQuery("SELECT pID, name, birthYear, deathYear, profession FROM people WHERE"
													+ " profession LIKE '%actor%' OR profession LIKE '%actress%';");
										}
										else {
											rs = stmt.executeQuery("SELECT pID, name, birthYear, deathYear, profession FROM people WHERE"
													+ " name LIKE '%" + nameToSearch + "%' AND (profession LIKE '%actor%' or profession "
													+ "LIKE '%actress%');");
										}
										ArrayList<String> pIDs = new ArrayList<String>();
										ArrayList<String> names = new ArrayList<String>();
										ArrayList<String> birthYears = new ArrayList<String>();
										ArrayList<String> deathYears = new ArrayList<String>();
										ArrayList<String> professions = new ArrayList<String>();
										while (rs.next()) {
											pIDs.add(rs.getString(1));
											names.add(rs.getString(2));
											birthYears.add(rs.getString(3));
											deathYears.add(rs.getString(4));
											professions.add(rs.getString(5));
										}
										System.out.println("Actor/Actress Search Results for: '" + nameToSearch + "'");
										for (int i = 0; i<pIDs.size();i++) {
											String alive = (deathYears.get(i) == null || deathYears.get(i).isEmpty()) ? "Present" : 
												deathYears.get(i); 
											String born = (birthYears.get(i) == null || birthYears.get(i).isEmpty()) ? "Unknown" : 
												birthYears.get(i);
											System.out.println(names.get(i) + " - (" + born + "-" + alive + ") - " + professions.get(i));
										}
										System.out.println();
										break;
									case 2:
										System.out.print("Enter a name to search for or press ENTER to show all directors: ");
										String nameToSearch2 = input.nextLine();
										System.out.println();
										if (nameToSearch2.equals("")) {
											rs = stmt.executeQuery("SELECT pID, name, birthYear, deathYear, profession FROM people WHERE"
													+ " profession LIKE '%director%';");
										}
										else {
											rs = stmt.executeQuery("SELECT pID, name, birthYear, deathYear, profession FROM people WHERE"
													+ " name LIKE '%" + nameToSearch2 + "%' AND profession LIKE '%director%' ;");
										}
										ArrayList<String> pIDs2 = new ArrayList<String>();
										ArrayList<String> names2 = new ArrayList<String>();
										ArrayList<String> birthYears2 = new ArrayList<String>();
										ArrayList<String> deathYears2 = new ArrayList<String>();
										ArrayList<String> professions2 = new ArrayList<String>();
										while (rs.next()) {
											pIDs2.add(rs.getString(1));
											names2.add(rs.getString(2));
											birthYears2.add(rs.getString(3));
											deathYears2.add(rs.getString(4));
											professions2.add(rs.getString(5));
										}
										System.out.println("Director Search Results for: '" + nameToSearch2 + "'");
										for (int i = 0; i<pIDs2.size();i++) {
											String alive = (deathYears2.get(i) == null || deathYears2.get(i).isEmpty()) ? "Present" : 
												deathYears2.get(i); 
											String born = (birthYears2.get(i) == null || birthYears2.get(i).isEmpty()) ? "Unknown" : 
												birthYears2.get(i);
											System.out.println(names2.get(i) + " - (" + born + "-" + alive + ") - "+ professions2.get(i));
										}
										System.out.println();
										break;
									case 3:
										System.out.print("Enter a name to search for or press ENTER to show all producers: ");
										String nameToSearch3 = input.nextLine();
										System.out.println();
										if (nameToSearch3.equals("")) {
											rs = stmt.executeQuery("SELECT pID, name, birthYear, deathYear, profession FROM people WHERE"
													+ " profession LIKE '%producer%';");
										}
										else {
											rs = stmt.executeQuery("SELECT pID, name, birthYear, deathYear, profession FROM people WHERE"
													+ " name LIKE '%" + nameToSearch3 + "%' AND profession LIKE '%producer%' ;");
										}
										ArrayList<String> pIDs3 = new ArrayList<String>();
										ArrayList<String> names3 = new ArrayList<String>();
										ArrayList<String> birthYears3 = new ArrayList<String>();
										ArrayList<String> deathYear3 = new ArrayList<String>();
										ArrayList<String> professions3 = new ArrayList<String>();
										while (rs.next()) {
											pIDs3.add(rs.getString(1));
											names3.add(rs.getString(2));
											birthYears3.add(rs.getString(3));
											deathYear3.add(rs.getString(4));
											professions3.add(rs.getString(5));
										}
										System.out.println("Producer Search Results for: '" + nameToSearch3 + "'");
										for (int i = 0; i<pIDs3.size();i++) {
											String alive = (deathYear3.get(i) == null || deathYear3.get(i).isEmpty()) ? "Present" : 
												deathYear3.get(i); 
											String born = (birthYears3.get(i) == null || birthYears3.get(i).isEmpty()) ? "Unknown" : 
												birthYears3.get(i);
											System.out.println(names3.get(i) + " - (" + born + "-" + alive + ") - "+ professions3.get(i));
										}
										System.out.println();
										break;
									case 4:
										System.out.println("Enter a name to search for or press enter to show all actors");
										String nameToSearch4 = input.nextLine();
										System.out.println();
										if (nameToSearch4.equals("")) {
											rs = stmt.executeQuery("SELECT pID, name FROM people");
										}
										else {
											rs = stmt.executeQuery("SELECT pID, name FROM people WHERE name LIKE '%" + nameToSearch4 + 
													"%'");
										}
										ArrayList<String> pIDs4 = new ArrayList<String>();
										ArrayList<String> names4 = new ArrayList<String>();
										while (rs.next()) {
											pIDs4.add(rs.getString(1));
											names4.add(rs.getString(2));
										}
										System.out.println("Which person's movies would you like to see?");
										for (int i = 0; i<pIDs4.size();i++) {
											System.out.println("[" + (i+1) + "] " + names4.get(i));
										}
										System.out.print("Enter a number: ");
										int personSearchIndex = input.nextInt();
										System.out.println();
										if (personSearchIndex > 0 && personSearchIndex <= pIDs4.size()) {
											rs = stmt.executeQuery("SELECT name, title, cast.role FROM people JOIN cast USING(pID)"
													+ " JOIN Movies USING (mID) WHERE Cast.pID='" + pIDs4.get(personSearchIndex-1) + "'");
											System.out.println(names4.get(personSearchIndex-1) + " was involved in the following movies: ");
											while (rs.next()) {
												System.out.println(rs.getString(2) + " - " + rs.getString(3));
											}
										}
										System.out.println();
										break;
									case 5:
										System.out.println("Which year would you like to look at?");
										rs = stmt.executeQuery("SELECT DISTINCT(releaseYear) FROM Movies ORDER BY releaseYear DESC;");
										ArrayList<String> years = new ArrayList<String>();
										int j = 1;
										while (rs.next()) {
											years.add(rs.getString(1));
											System.out.println("[" + (j) + "] " + rs.getString(1));
											j++;
										}
										System.out.print("Enter a number: ");
										int yearIndex = input.nextInt();
										System.out.println();
										String properYear = years.get(yearIndex-1);
										rs = stmt.executeQuery("SELECT DISTINCT(name) FROM People JOIN Cast USING (pID) JOIN Movies USING"
												+ " (mID) WHERE releaseYear= " + properYear + " AND name NOT IN (SELECT name FROM People "
												+ "JOIN Cast USING (pID) JOIN Movies USING (mID) WHERE releaseYear <> " + properYear +")");
										
										ArrayList<String> names5 = new ArrayList<String>();
										while (rs.next()) {
											names5.add(rs.getString(1));
										}
										System.out.println("The following people were only involved in movies from: " + properYear);
										for (int i = 0; i<names5.size();i++) {
											System.out.println(names5.get(i));
										}
										System.out.println();
										break;
									default:
										break;
								}
							}
							operation = -1;
							break;
						default:
							break;
					}	
				}
			}
			else { // Admin
				while (operation != 0) {
					System.out.print("[1] Add showtimes\n[2] Delete showtimes\n[3] Archive showtimes\n"
							+ "[4] Delete movies\n[5] Delete users\n[6] View Archives\n[0] Logout\nEnter a number: ");
					operation = input.nextInt();
					input.nextLine();
					System.out.println();
					switch (operation) { 
						case 1:
							ArrayList<String> defaultStartTimes = new ArrayList<String>();
							defaultStartTimes.add("08:00:00");
							defaultStartTimes.add("10:00:00");
							defaultStartTimes.add("12:00:00");
							defaultStartTimes.add("14:00:00");
							defaultStartTimes.add("16:00:00");
							defaultStartTimes.add("18:00:00");
							defaultStartTimes.add("20:00:00");
							defaultStartTimes.add("22:00:00");
							rs = stmt.executeQuery("SELECT mID, title FROM Movies");
							ArrayList<String> showTimeMovies = new ArrayList<String>();
							ArrayList<String> showTimeMovieIDs = new ArrayList<String>();
							while (rs.next()) {
								showTimeMovieIDs.add(rs.getString(1));
								showTimeMovies.add(rs.getString(2));
							}
							System.out.println("Which movie would you like to add a showtime for?");
							for (int i = 0; i<showTimeMovies.size();i++) {
								System.out.println("[" + (i+1) + "] " + showTimeMovies.get(i));
							}
							System.out.println("[0] GO-BACK-TO-MENU");
							System.out.print("Enter a number: ");
							int showTimeMovieIndex = input.nextInt();
							System.out.println();
							if (showTimeMovieIndex > 0 && showTimeMovieIndex <= showTimeMovieIDs.size()) {
								System.out.println("Please select a start time for: " + showTimeMovies.get(showTimeMovieIndex-1) + "\n");
								for (int i = 0; i<defaultStartTimes.size();i++) {
									System.out.println("[" + (i+1) + "] " + defaultStartTimes.get(i));
								}
								System.out.println("[0] GO-BACK-TO-MENU");
								System.out.print("Enter a number: ");
								int showTimeIndex = input.nextInt();
								System.out.println();
								if (showTimeIndex > 0 && showTimeIndex <= defaultStartTimes.size()) {
									stmt.executeUpdate("INSERT INTO ShowTimes VALUES(NULL, '" + showTimeMovieIDs.get(showTimeMovieIndex-1)
										+ "', '" + defaultStartTimes.get(showTimeIndex-1) + "', current_timestamp)");
									System.out.println("Successfully created showtime for '" + showTimeMovies.get(showTimeMovieIndex-1) 
										+ "' at " + defaultStartTimes.get(showTimeIndex-1) + "\n");
								}
							}
							break;
						case 2:
							rs = stmt.executeQuery("SELECT sID, mID, startTime, title FROM ShowTimes JOIN movies USING (mID);");
							ArrayList<String> showTimes = new ArrayList<String>();
							ArrayList<String> movieList = new ArrayList<String>();
							ArrayList<String> startTimes = new ArrayList<String>();
							ArrayList<String> movieTitles = new ArrayList<String>();
							while (rs.next()) {
								showTimes.add(rs.getString(1));
								movieList.add(rs.getString(2));
								startTimes.add(rs.getString(3));
								movieTitles.add(rs.getString(4));
							}
							System.out.println("Which showtime would you like to delete?");
							for (int i = 0; i<showTimes.size();i++) {
								System.out.println("[" + (i+1) + "] " + movieTitles.get(i) + " - " + startTimes.get(i));
							}
							System.out.println("[0] GO-BACK-TO-MENU");
							System.out.print("Enter a number: ");
							int deleteShowTime = input.nextInt();
							System.out.println();
							if (deleteShowTime > 0 && deleteShowTime <= showTimes.size()) {
								stmt.executeUpdate("DELETE FROM ShowTimes WHERE sID=" + showTimes.get(deleteShowTime-1) + ";");
								System.out.println("Successfully deleted showtime: " + movieTitles.get(deleteShowTime-1) + " - " 
										+ startTimes.get(deleteShowTime-1) + "\n");
							}
							break;
						case 3:
							String sql = "{call archiveShowTimes(?)}";
							CallableStatement cstmt = conn.prepareCall(sql);
							
							System.out.print("Input month (number): ");
							int userMonth = input.nextInt();
							System.out.print("Input day (number): ");
							int userDay = input.nextInt();
							System.out.print("Input year (number): ");
							int userYear = input.nextInt();
							
							Date dateObj = new Date(userYear - 1900, userMonth - 1, userDay);
							cstmt.setDate("archiveDate", dateObj);
							cstmt.execute();
							String dateString = userMonth + "-" + userDay + "-" + userYear;
							System.out.println("Successfully archived Show Times before: " + dateString + "\n");
							break;
						case 4:
							rs = stmt.executeQuery("SELECT title FROM Movies");
							ArrayList<String> movies = new ArrayList<String>();
							while (rs.next()) {
								movies.add(rs.getString(1));
							}
							System.out.println("Which Movie would you like to delete?");
							for (int i = 0; i<movies.size();i++) {
								System.out.println("[" + (i+1) + "] " + movies.get(i));
							}
							System.out.println("[0] GO-BACK-TO-MENU\n");
							System.out.print("Enter a number: ");
							int deleteMovie = input.nextInt();
							System.out.println();
							if (deleteMovie > 0 && deleteMovie <= movies.size()) {
								stmt.executeUpdate("DELETE FROM Movies WHERE title LIKE '%" + movies.get(deleteMovie-1) + "%';");
								System.out.println("Successfully deleted movie: " + movies.get(deleteMovie-1) + "\n");
							}
							break;
						case 5:
							rs = stmt.executeQuery("SELECT username FROM users");
							ArrayList<String> users = new ArrayList<String>();
							while (rs.next()) {
								users.add(rs.getString(1));
							}
							System.out.println("Which user would you like to delete?");
							for (int i = 0; i<users.size();i++) {
								System.out.println("[" + (i+1) + "] " + users.get(i));
							}
							System.out.println("[0] GO-BACK-TO-MENU\n");
							System.out.print("Enter a number: ");
							int delete = input.nextInt();
							System.out.println();
							if (delete > 0 && delete <= users.size()) {
								if (username.equals(users.get(delete-1))) {
									System.out.println("You cannot delete yourself!");
								}
								else {
									stmt.executeUpdate("DELETE FROM Users WHERE username='" + users.get(delete-1) + "';");
									System.out.println("Successfully deleted user: " + users.get(delete-1) + "\n");
								}
							}
							break;
						case 6:
							rs = stmt.executeQuery("SELECT * FROM Archive;");
							System.out.println("Archived Showtimes:");
							while (rs.next()) {
								System.out.println(rs.getString(1) + ", " + rs.getString(2) + ", " + rs.getString(3) + ", " 
										+ rs.getString(4) + ", ");
							}
							System.out.println();
							break;
						default:
							break;
					}	
				}
			}
		}
		catch(SQLException se) { // Handle errors for JDBC
		    se.printStackTrace();
		}
		finally { // close resources
		    try {
		    	if (stmt != null)
		    		stmt.close();
		    }
		    catch(SQLException se) {
		    	se.printStackTrace();
		    }
		    try {
		        if (conn != null) {
		        	conn.close();
		        }	            
		    }
		    catch(SQLException se) {
		         se.printStackTrace();
		    }
		    input.close();
		    System.out.println("=================================================");
		    System.out.println("Thank you for using the Movie Reservation system!");
		}
	 }	
}