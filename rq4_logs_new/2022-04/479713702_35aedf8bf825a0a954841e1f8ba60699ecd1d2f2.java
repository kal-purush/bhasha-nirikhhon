/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package dao;

import entity.Category;
import entity.NewsLetter;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author mabea
 */
public class NewsLetterDao extends DBConnection{
    public NewsLetter findById(int id){
        NewsLetter c = null;
        try {
            Statement st = this.getConnection().createStatement();
            String query = "select * from category where id="+id;
            
            ResultSet rs = st.executeQuery(query);
            while(rs.next()){
                c = new NewsLetter(rs.getInt("id"),rs.getString("mail"),rs.getBoolean("status"));
            }
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return c;
    }
        
    public void create(NewsLetter c){
        try {
            Statement st = this.getConnection().createStatement();
            String query = "insert into news_letter(mail,status) values ('"+c.getMail()+"','"+c.getStatus()+"')";
            st.executeUpdate(query);
        
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public void delete(NewsLetter c){
        try {
            Statement st = this.getConnection().createStatement();
            String query = "delete from news_letter where id="+c.getId();
            st.executeUpdate(query);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public void update(NewsLetter c){
        try {
            Statement st = this.getConnection().createStatement();
            String query = "update news_letter set mail='"+c.getMail()+"' where id="+c.getId()+"' where status="+c.getStatus();
            st.executeUpdate(query);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public List<NewsLetter> getList(){
        List<NewsLetter> list = new ArrayList<>();
        try {
            Statement st = this.getConnection().createStatement();
            String query = "select * from news_letter";
            
            ResultSet rs = st.executeQuery(query);
            while(rs.next()){
                list.add(new NewsLetter(rs.getInt("id"),rs.getString("mail"),rs.getBoolean("status")));
            }
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return list;
    }
}