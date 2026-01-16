/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package DAO;

import Context.DBContext;
import Entity.Question;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

/**
 *
 * @author HP
 */
public class QuestionDAOImplement extends DBContext implements QuestionDAO{

    @Override
    public int addListQuestTionByQuizID(int qId, List<Question> listq) {
        int Count=0;
        try {
            Connection con = getConnection();
            String sql="";
            PreparedStatement ps = con.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
            
        } catch (Exception e) {
        }
        return Count;
    }
    
}