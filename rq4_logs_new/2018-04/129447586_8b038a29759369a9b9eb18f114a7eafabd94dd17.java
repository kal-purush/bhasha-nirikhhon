package sample;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.ListView;
import javafx.stage.Stage;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.ResourceBundle;

public class GetAssign extends Application implements Initializable
{
    public static Stage prim_stage;
    public static int proj_id;
    public ListView<String> assg_list;
    public ArrayList<String> assglist;
    public GetAssign() throws Exception
    {
        assglist= new ArrayList<>();
        //Fetch Project List
        Class.forName("com.mysql.jdbc.Driver");

        Connection conn  = DriverManager.getConnection("jdbc:mysql://localhost:3306/project?useSSL=false","root","karan");

        Statement stmt  = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select task,resource,progress from assignments where proj_id="+proj_id);
        while(rs.next())
            assglist.add(rs.getString(1) +" "+rs.getString(2) + " Progress: "+rs.getFloat(3));
        conn.close();
    }
    @Override
    public void start(Stage primaryStage) throws Exception
    {
        Parent root = FXMLLoader.load(getClass().getResource("getAssign.fxml"));
        primaryStage.setTitle("Get Assignments");
        primaryStage.setScene(new Scene(root, 640, 480));
        primaryStage.show();

    }

    @Override
    public void initialize(URL location, ResourceBundle resources)
    {

        ObservableList<String> assg = FXCollections.observableArrayList(assglist);
        assg_list.setItems(assg);

    }
}