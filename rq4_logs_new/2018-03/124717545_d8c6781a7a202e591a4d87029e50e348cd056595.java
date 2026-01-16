import static org.json.simple.JSONObject.escape;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Populate {
    private static final String businessFilePath = "C:\\xyz\\yelp_business.json";
    private static final String checkinFile = "C:\\xyz\\yelp_checkin.json";
    private static final String reviewFile = "C:\\yelp_review.json";
    private static final String userFile = "C:\\xyz\\yelp_user.json";

    private static PreparedStatement pstmt;
    private static Connection conn;
    private static JSONParser jsonParser;

    public static void main(String[] args) throws Exception {
        jsonParser = new JSONParser();
        conn = connection();
        createYelpUserTable();
        createBusinessTable();
        createCheckinTable();
        createReviewTable();
        conn.close();
    }
    private static void createYelpUserTable() {
        // TODO Auto-generated method stub
        // Fetching the user information out of the JSON file starts HERE
        // #########################################################################################

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(userFile))) {
            String line = null;
            int count = 0;
            while ((line = bufferedReader.readLine()) != null) {

                JSONObject jsonObject = (JSONObject) jsonParser.parse(line);

                // Fetching the votes info starts
                JSONObject votes = (JSONObject) jsonObject.get("votes");
                Set keys = votes.keySet();
                Iterator a = keys.iterator();
                Long coolVotes = 0L;
                Long funnyVotes = 0L;
                Long usefulVotes = 0L;
                Long reviewCount = 0L;
                Long fans = 0L;
                Double averageStars = 0.0;
                while (a.hasNext()) {
                    String votes_det = (String) a.next();
                    Long times = (Long) votes.get(votes_det);
                    if (votes_det.equals("funny")) {
                        funnyVotes = times;
                    }
                    if (votes_det.equals("cool")) {
                        coolVotes = times;
                    }
                    if (votes_det.equals("useful")) {
                        usefulVotes = times;
                    }
                    System.out.println(votes_det);
                    System.out.println(times);
                }
                // Fetching the votes info ends

                // Fetching other objects starts
                String yelpingSince = (String) jsonObject.get("yelping_since");
                String name = (String) jsonObject.get("name");
                name = name.replace("'", "''");
                reviewCount = (Long) jsonObject.get("review_count");
                String userId = (String) jsonObject.get("user_id");
                String type = (String) jsonObject.get("type");
                fans = (Long) jsonObject.get("fans");
                averageStars = (Double) jsonObject.get("average_stars");

                // Fetching other objects ends

                // Inserting into database starts

                String sql1 = "INSERT INTO YelpUSER (YELPING_SINCE, FUNNYVOTES, USEFULVOTES, COOLVOTES, REVIEWCOUNT, USERNAME, USERID, FANS, AVERAGESTARS, USERTYPE) VALUES ("
                        + "'" + (yelpingSince) + "'" + "," + funnyVotes + "," + usefulVotes + "," + coolVotes + ","
                        + reviewCount + "," + "'" + name + "'," + "'" + userId + "'," + "'" + fans + "'," + averageStars
                        + "," + "'" + type + "'" + ") ";
                System.out.println(sql1);
                pstmt = conn.prepareStatement(sql1);
                pstmt.execute();

                pstmt.close();

                friendsTable(conn, jsonObject, userId);
            }
        } catch (Exception e) {
        }
    }

    private static void createBusinessTable() {
        // TODO Auto-generated method stub
        // //Fetching the business information out of the JSON file starts HERE
        // //#########################################################################################
        BufferedReader bufferedReader = null;
        JSONObject jsonObject = null;
        String bId = null;
        try {
            String line = null;
            int count = 0;
            bufferedReader = new BufferedReader(new FileReader(businessFilePath));
            while ((line = bufferedReader.readLine()) != null) {

                jsonObject = (JSONObject) jsonParser.parse(line);

                // Fetching the business ID starts
                bId = (String) jsonObject.get("business_id");
                System.out.println(bId);
                // Fetching the business ID ends

                // Fetching the full address starts
                String fullAddress = (String) jsonObject.get("full_address");
                fullAddress = fullAddress.replace("'", "''");
                System.out.println(fullAddress);
                // Fetching the full address ends

                // Checking if the shop is open or not starts
                Boolean openOrNot = (Boolean) jsonObject.get("open");
                System.out.println(openOrNot);
                // Checking if the shop is open or not ends

                // Fetching of other attributes starts

                Double longi = (Double) jsonObject.get("longitude");
                String state = (String) jsonObject.get("state");
                Double stars = (Double) jsonObject.get("stars");
                Double lat = (Double) jsonObject.get("latitude");
                Long reviewCount = (Long) jsonObject.get("review_count");
                String name = (String) jsonObject.get("name");
                name = name.replace("'", "''");
                String city = (String) jsonObject.get("city");
                String type = (String) jsonObject.get("type");
                System.out.println(longi);
                System.out.println(stars);
                System.out.println(state);
                System.out.println(lat);
                System.out.println(name);
                System.out.println(reviewCount);
                // Fetching the other attributes ends

                // Inserting into the business table starts
                String pss = "INSERT INTO Business(BusinessId,FullAddress,IsOpen,City,ReviewCount,BName,Longitude,State,Stars,Latitude,BType) VALUES ("
                        + "'" + bId + "'," + "'" + fullAddress + "'," + "'" + openOrNot + "'," + "'" + city + "',"
                        + reviewCount + "," + "'" + name + "'," + longi + "," + "'" + state + "'," + stars + "," + lat
                        + "," + "'" + type + "'" + ") ";
                System.out.println(pss);

                pstmt = conn.prepareStatement(pss);
                pstmt.execute();
                pstmt.close();

                // //Inserting into the business table ends

                JSONArray categories = (JSONArray) jsonObject.get("categories");
                Iterator<String> iterator = categories.iterator();
                String catString[];
                boolean x;
                catString = new String[] { "Active Life", "Arts & Entertainment", "Automotive", "Car Rental", "Cafes",
                        "Beauty & Spas", "Convenience Stores", "Dentists", "Doctors", "Drugstores", "Department Stores",
                        "Education", "Event Planning & Services", "Flowers & Gifts", "Food", "Health & Medical",
                        "Home Services", "Home & Garden", "Hospitals", "Hotels & Travel", "Hardware Stores", "Grocery",
                        "Medical Centers", "Nurseries & Gardening", "Nightlife", "Restaurants", "Shopping",
                        "Transportation" };
                try {
                    while (iterator.hasNext()) {
                        x = false;
                        String catname = iterator.next().replace("'", "''");
                        for (int item = 0; item < 28; item++) {
                            if (catString[item].equals(catname)) {
                                x = true;
                                break;
                            }
                        }

                        if (x == false) {
                            String cat = "INSERT INTO BusinessSubCategory(SubCategoryName,BusinessId) VALUES (" + "'" + catname
                                    + "'," + "'" + bId + "'" + ") ";
                            System.out.println(cat);
                            pstmt = conn.prepareStatement(cat);
                            pstmt.execute();
                            pstmt.close();
                        } else {
                            String cat = "INSERT INTO BusinessCategory(CategoryName,BusinessId) VALUES (" + "'" + catname + "',"
                                    + "'" + bId + "'" + ") ";
                            System.out.println(cat);
                            pstmt = conn.prepareStatement(cat);
                            pstmt.execute();
                            pstmt.close();
                        }
                    }
                } catch (Exception e) {
                }

            }

        } catch (Exception e) {
        }

    }
    private static void createCheckinTable() {
        // TODO Auto-generated method stub
        // //Fetching the checkin info starts

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(checkinFile))) {
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                JSONObject jsonObject = (JSONObject) jsonParser.parse(line);
                String checkinType=(String) jsonObject.get("type");
                String bId=(String) jsonObject.get("business_id");
                JSONObject checkinInfo = (JSONObject) jsonObject.get("checkin_info");
                Set keys = checkinInfo.keySet();
                Iterator a = keys.iterator();
                while (a.hasNext()) {
                    String date = (String) a.next();
                    Long times = (Long) checkinInfo.get(date);

                    String arrDay[] = new String[] { "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
                            "Saturday" };
                    String[] arrStr = date.split("-", 2);
                    int hour = Integer.parseInt(arrStr[0]);
                    String checkinDay = arrDay[Integer.parseInt(arrStr[1])];
                    String pss = "INSERT INTO checkin(BusinessId,CheckinHour,CheckinDay,CheckinCount) VALUES (" + "'" + bId + "',"
                            + hour + "," + "'" + checkinDay + "'," + times + ") ";
                    System.out.println(pss);
                    pstmt = conn.prepareStatement(pss);
                    pstmt.execute();
                    pstmt.close();
                }
            }
        } catch (Exception e) {
        }
        // Fetching the checkin info ends

    }

    private static void createReviewTable() {

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(reviewFile))) {
            String line = null;
            int count = 0;
            while ((line = bufferedReader.readLine()) != null) {

                JSONObject jsonObject = (JSONObject) jsonParser.parse(line);

                // Fetching the votes info starts
                Long funnyVote = 0L;
                Long coolVote = 0L;
                Long usefulVote = 0L;
                JSONObject votes = (JSONObject) jsonObject.get("votes");
                Set keys = votes.keySet();
                Iterator a = keys.iterator();
                while (a.hasNext()) {
                    String votes_det = (String) a.next();
                    Long times = (Long) votes.get(votes_det);
                    System.out.println(votes_det);
                    System.out.println(times);
                    if (votes_det.equals("funny")) {
                        funnyVote = times;
                    }
                    if (votes_det.equals("cool")) {
                        coolVote = times;
                    }
                    if (votes_det.equals("useful")) {
                        usefulVote = times;
                    }
                }
                // Fetching the votes info ends

                // Fetching other two objects starts
                String userId = (String) jsonObject.get("user_id");
                String reviewId = (String) jsonObject.get("review_id");
                Long stars = (Long) jsonObject.get("stars");
                String date = (String) jsonObject.get("date");
                String text = (String) jsonObject.get("text");
                text = text.replace("'", "''");
                String type = (String) jsonObject.get("type");
                String bId=(String)jsonObject.get("business_id");
                String sql = "INSERT INTO Review(FunnyVote,UsefulVote,CoolVote,UserId,ReviewId,Stars,RDate,RText,RType,BusinessId) VALUES (?,?,?,?,?,?,?,?,?,?)";
                pstmt = conn.prepareStatement(sql);
                pstmt.setLong(1, funnyVote);
                pstmt.setLong(2, usefulVote);
                pstmt.setLong(3, coolVote);
                pstmt.setString(4, userId);
                pstmt.setString(5, reviewId);
                pstmt.setLong(6, stars);
                pstmt.setString(7, date);
                pstmt.setString(8, escape((String) jsonObject.get("text")));
                pstmt.setString(9, type);
                pstmt.setString(10, bId);
                pstmt.execute();
                pstmt.close();
            }
        } catch (Exception e) {
        }
    }

    private static void friendsTable(Connection conn2, JSONObject jsonObject, String userId) {
        // TODO Auto-generated method stub
        JSONArray friends = (JSONArray) jsonObject.get("friends");
        Iterator<String> fri = friends.iterator();
        while (fri.hasNext()) {
            String sql2 = "INSERT INTO Friends (UserId, FriendId) VALUES (" + "'" + userId + "'," + "'" + fri.next()
                    + "'" + ") ";
            System.out.println(sql2);
            try {
                pstmt = conn.prepareStatement(sql2);
                pstmt.execute();
                pstmt.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
    }
    private static Connection connection() {
        // TODO Auto-generated method stub

        Connection conn = null;
        try {

            // Establish a connection
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:orcl", "vardan", "XXXX");
        } catch (Exception e) {
            System.err.println("Problem with connection");
            System.err.println(e.getMessage());
        }
        return conn;
    }



}