    /*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package dao;
import entity.Blog;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
/**
 *
 * @author Gökhan
 */
public class BlogDao extends DBConnection{
    public Blog findById(int id){
        Blog c = null;
        try {
            Statement st = this.getConnection().createStatement();
            String query = "select * from blog where id="+id;
            
            ResultSet rs = st.executeQuery(query);
            while(rs.next()){
                c = new Blog(rs.getInt("id"),rs.getString("title"),rs.getString("content"),rs.getString("image"),
                        rs.getDate("create_date"),rs.getInt("category_id"),rs.getBoolean("status"),rs.getInt("writer_id"));
            }
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return c;
    }
        
    public void create(Blog c){
        try {
            Statement st = this.getConnection().createStatement();
            String query = "insert into blog(title,content,image,create_date,category_id,status,writer_id) values "
                    + "('"+c.getTitle()+"','"+c.getContent()+"','"+c.getImage()+"','"+c.getCreate_date()+"','"+c.getCategory_id()+"','"+c.getStatus()+"'"
                    + ",'"+c.getWriter_id()+"'";
            st.executeUpdate(query);
        
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public void delete(Blog c){
        try {
            Statement st = this.getConnection().createStatement();
            String query = "delete from blog where id="+c.getId();
            st.executeUpdate(query);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public void update(Blog c){
        try {
            Statement st = this.getConnection().createStatement();
            String query = "update blog set title='"+c.getTitle()+"' where id="+c.getId()+"' where content="+c.getContent()
                    +"' where image="+c.getImage()
                    +"' where create_date="+c.getCreate_date()
                    +"' where category_id="+c.getCategory_id()
                    +"' where status="+c.getStatus()
                    +"' where writer_id="+c.getWriter_id();
            st.executeUpdate(query);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public List<Blog> getList(){
        List<Blog> list = new ArrayList<>();
        try {
            Statement st = this.getConnection().createStatement();
            String query = "select * from blog";
            
            ResultSet rs = st.executeQuery(query);
            while(rs.next()){
                list.add(new Blog(rs.getInt("id"),rs.getString("title"),rs.getString("content"),rs.getString("image"),
                        rs.getDate("create_date"),rs.getInt("category_id"),rs.getBoolean("status"),rs.getInt("writer_id")));
            }
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return list;
    }
}