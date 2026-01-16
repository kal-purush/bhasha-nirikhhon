package com.example.englishforkids.dao;

import com.example.englishforkids.model.Speaking;
import com.example.englishforkids.myconnection.MySQLConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class SpeakingDAO extends EngSysDAO<Speaking, String>{
    final public static String SELECT_ALL_SPEAKING_IN_LESSON_QUERY =
            "SELECT s.IdSpeaking, s.Title, s.Content, s.Example\n" +
                    "FROM SPEAKING s\n" +
                    "INNER JOIN SPEAKINGPART sp ON s.IdSpeaking = sp.IdSpeaking\n" +
                    "INNER JOIN LESSONPART lp ON sp.IdLessonPart = lp.IdLessonPart\n" +
                    "INNER JOIN LESSON l ON lp.IdLesson = l.IdLesson\n" +
                    "WHERE l.IdLesson = ?;";
    public boolean insert(Speaking entity){
        return false;
    }

    public void update(Speaking entity){

    }

    public void delete(String id){

    }

    public Speaking selectById(String id){
        return null;
    }

    public List<Speaking> selectAll(){
        return null;
    }

    public List<Speaking> selectBySql(String sql, Object...args){
        LinkedList<Speaking> lstSpeaking = new LinkedList<>();
        Connection connection = MySQLConnection.getConnection();
        if (connection != null) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                for (int i = 0; i < args.length; i++) {
                    preparedStatement.setObject(i + 1, args[i]);
                }
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Speaking speaking = new Speaking();
                    speaking.setIdSpeaking(resultSet.getString("IdSpeaking"));
                    speaking.setTitle(resultSet.getString("Title"));
                    speaking.setContent(resultSet.getString("Content"));
                    speaking.setExample(resultSet.getBytes("Example"));
                    lstSpeaking.add(speaking);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return lstSpeaking;
    }
}