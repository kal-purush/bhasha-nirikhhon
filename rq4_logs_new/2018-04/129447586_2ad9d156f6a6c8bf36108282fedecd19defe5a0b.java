package sample;

import javafx.application.Application;
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

public class AllocateTasks extends Application implements Initializable
{
    public static Stage primStage;
    public ListView<String>  task_list;
    public ListView<String> res_list;
    private ArrayList<String> tasksList;
    private ArrayList<String> resList;
    public static int proj_id;

    public AllocateTasks() throws Exception
    {

        tasksList= new ArrayList<>();
        resList = new ArrayList<>();
        //Fetch Project List
        Class.forName("com.mysql.jdbc.Driver");

        Connection conn  = DriverManager.getConnection("jdbc:mysql://localhost:3306/project?useSSL=false","root","karan");

        Statement stmt  = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select name from resource where proj_id ="+proj_id);
        while(rs.next())
            resList.add(rs.getString(1));
        conn.close();

    }

    @Override
    public void start(Stage primaryStage) throws Exception
    {
        primStage=primaryStage;
        Parent root = FXMLLoader.load(getClass().getResource("allocateTasks.fxml"));
        primaryStage.setTitle("Allocate Tasks");
        primaryStage.setScene(new Scene(root, 640, 480));
        primaryStage.show();
    }

    @Override
    public void initialize(URL location, ResourceBundle resources)
    {

    }
}