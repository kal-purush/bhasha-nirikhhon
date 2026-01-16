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
import java.sql.*;
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


         stmt  = conn.createStatement();
        rs = stmt.executeQuery("select name from tasks where proj_id ="+proj_id);
        while(rs.next())
            tasksList.add(rs.getString(1));


        conn.close();

    }
    public void createAssignment() throws Exception
    {
        Assignment assg = new Assignment(task_list.getSelectionModel().getSelectedItem(),res_list.getSelectionModel().getSelectedItem(),proj_id);

        Connection conn  = DriverManager.getConnection("jdbc:mysql://localhost:3306/project?useSSL=false","root","karan");

        String query = " insert into assignments "
                + " values (?, ?, ?,?)";

        // Insert Project into database

        PreparedStatement preparedStmt = conn.prepareStatement(query);
        preparedStmt.setString (1,assg.task );
        preparedStmt.setString (2, assg.resource);
        preparedStmt.setInt  (3, assg.proj_id);
        preparedStmt.setFloat(4,0);
        // execute the preparedstatement
        preparedStmt.execute();
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

        ObservableList<String> res = FXCollections.observableArrayList(resList);
        res_list.setItems(res);

        ObservableList<String> task = FXCollections.observableArrayList(tasksList);
        task_list.setItems(task);




    }
}