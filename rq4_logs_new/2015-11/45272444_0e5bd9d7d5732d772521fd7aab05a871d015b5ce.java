package is.ru.tictactoe;

import java.io.StringWriter;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import static spark.Spark.*;
import spark.servlet.SparkApplication;


public class TicTacToeWebApp implements SparkApplication {

   private TicTacToeLogic logic;

   public static void main(String[] args) {
   		
   	  staticFileLocation("/public");
        SparkApplication ticWeb = new TicTacToeWebApp();

        String port = System.getenv("PORT");
        if (port != null) {
            port(Integer.valueOf(port));
        }

        ticWeb.init();
   }

   @Override
    public void init() {

      String layout = "templates/layout.vtl";

         get("/", (req, res) -> {
            logic = null;
            return getPage("index", new VelocityContext());
         });

         get("/play", (req, res) -> {
            TicTacToePlayer p1 =  new TicTacToePlayer(req.queryParams("p1name"));
            TicTacToePlayer p2 =  new TicTacToePlayer(req.queryParams("p2name"));
            logic = new TicTacToeLogic(p1, p2);
            return getPage("game", getGameInfo());
         });
    }

    private String getPage(String pageName, VelocityContext context) {
      Template template = getTemplate(pageName);
      StringWriter writer = new StringWriter();
      
      template.merge(context, writer);
      context = new VelocityContext();
      context.put("template", writer.toString());


      template = getTemplate("layout");
      writer = new StringWriter();
      template.merge(context, writer);
      return writer.toString();
    }

    private VelocityContext getGameInfo() {
      VelocityContext context = new VelocityContext();
      getPlayerInfo(context);
      getGridInfo(context);
      return context;
    }

    private void getPlayerInfo(VelocityContext ve) {
      ArrayList playerInfo = new ArrayList();
      Map map = new HashMap();
      map.put("name", logic.getPlayer1Name());
      map.put("score", logic.getPlayer1Score());
      playerInfo.add(map);
      map = new HashMap();
      map.put("name", logic.getPlayer2Name());
      map.put("score", logic.getPlayer2Score());
      playerInfo.add(map);
      ve.put("playerInfo", playerInfo);
    }

    private void getGridInfo(VelocityContext ve) {
      ArrayList gridInfo = new ArrayList();
      Character[] grid = logic.getGrid();
      Map map;

      for(Character token : grid) {
         map = new HashMap();
         
         if(token == null)
            map.put("token", "");
         else
            map.put("token", token.toString());

         gridInfo.add(map);
      }

      ve.put("gridInfo", gridInfo);
    }

    private Template getTemplate(String templateName) {
      VelocityEngine ve = new VelocityEngine();
      ve.init();
      return ve.getTemplate("./src/main/resources/templates/" + templateName + ".vm");
    }
}