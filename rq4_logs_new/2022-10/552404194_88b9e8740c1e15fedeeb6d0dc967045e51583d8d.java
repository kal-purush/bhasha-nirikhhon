import com.google.gson.JsonObject;

import java.sql.*;

public class CheckLoadState {

    private static Connection getConnection() throws SQLException {
        String url = "jdbc:mysql://172.1.0.8/sys";
        String username = "home";
        String password ="locale";
        return DriverManager.getConnection(url, username, password);
    }

    public static JsonObject main(JsonObject args) throws SQLException {

        //parametri action
        String appUser = args.getAsJsonPrimitive("username").getAsString(),
                postId = args.getAsJsonPrimitive("pid").getAsString();

        System.out.println("Connessione al database...");

        try {
            Connection connection = getConnection();
            System.out.println("Database connesso!");



            //polling su nuovi log
            PreparedStatement select = connection.prepareStatement("select  COUNT(DISTINCT CASE WHEN ESITO = 'OK' THEN APP_PHASE END) AS OK_CK , COUNT(DISTINCT CASE WHEN ESITO = 'ERR' THEN APP_PHASE END) AS ERR_CK  from sys.users_logs where app_phase in ('IMG_COMPRESSION' ,'DESC_FILTERING') AND POST_ID = ? AND USERNAME = ?");
            select.setString(1, postId);
            select.setString(2, appUser);

            ResultSet rs = null;
            //polling su db per controllo errori su esecuzioni parallele
           int errCk = 0,  doneCk = 0,  step = 0;

           //controlla lo stato di esecuzione a intervalli di 1 sec fino a un massimo di 60 secondi
           while((errCk + doneCk) < 2 && step < 60)
           {
               Thread.sleep(1000);

               rs = select.executeQuery();
               rs.next();
               doneCk = rs.getInt("OK_CK");
               errCk = rs.getInt("ERR_CK");

               step++;
           }


            String tml ="https://localhost:31001/api/v1/web/guest/default/USER_GRID?username=" + appUser;
            String err ="https://localhost:31001/api/v1/web/guest/default/ADD_POST?user=" + appUser;
            String content = null;
            String status = null;

            //caro errore
           if(errCk > 0) {

               content = "<html> <body style=\"color: rgba(140, 80, 80, 0.9)\" > <h2>Si è verificato un errore nel caricamento.. </h2> <input type=\"button\" value=\"Clicca per riprovare ad aggiungere un post\" onClick =\"window.location.href = '" + err + "'\">  </body> </html>";;
               PreparedStatement delete = connection.prepareStatement("delete from sys.users_posts WHERE POST_ID = ? AND USERNAME = ?");
               delete.setString(1, postId);
               delete.setString(2, appUser);
               delete.executeUpdate();

               status = "400";

           }else if (doneCk == 2) { //OK

               content = "<html> <body style=\"color: rgba(140, 80, 80, 0.9)\" onload=\"window.location.replace('" + tml + "')\"> <h2>Immagine caricata con successo, redirezione alla tua bacheca in corso.. </h2> </body> </html>";
               PreparedStatement update = connection.prepareStatement("update sys.users_posts set update_date = CURRENT_TIMESTAMP WHERE POST_ID = ? AND USERNAME = ?");
               update.setString(1, postId);
               update.setString(2, appUser);
               update.executeUpdate();

               status = "200";

           }else { //errore grave
               status = "500";
               content = "<html> <body style=\"color: rgba(140, 80, 80, 0.9)\" > <h1>Errore critico di sistema </h1></body> </html>";
           }
            JsonObject response = new JsonObject();
            response.addProperty("body", content);
            response.addProperty("statusCode", status);

           return response;

        } catch (Exception e) {

            e.printStackTrace();
            throw new IllegalStateException("Errore durante l' interazione col DB!", e);
        }
    }

}