/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package fbird.cart;
import fbird.cart.CartDTO;
import fbird.utils.DBUtils;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
/**
 *
 * @author Admin
 */
public class CartDAO {
    private static final String VIEW_CART= "SELECT cart_item_id, quantity, product_image.image_1, cart_item.optional_shop_product_item_id, optional_shop_product_item.name, optional_shop_product_item.inventory, product_category.category_name, optional_shop_product_item.price, shop_product_item.title, shop_owner.shop_id, shop_owner.shop_name\n" +
"FROM  cart_item \n" +
"JOIN optional_shop_product_item  ON cart_item.optional_shop_product_item_id = optional_shop_product_item.optional_shop_product_item_id \n" +
"JOIN shop_product_item ON optional_shop_product_item.shop_product_item_id = shop_product_item.shop_product_item_id\n" +
"JOIN product_image on optional_shop_product_item.shop_product_item_id = product_image.shop_product_item_id\n" +
"JOIN shop_owner on shop_product_item.shop_id=shop_owner.shop_id\n" +
"JOIN product_category ON product_category.category_id = shop_product_item.category_id Where customer_id=?";
    private static final String ADD_CART_ITEM= "INSERT INTO cart_item (quantity, customer_id, optional_shop_product_item_id) VALUES(?,?,?)";
    private static final String DELETE_CART_ITEM="DELETE FROM cart_item WHERE cart_item_id = ?";
    
    private static final String ADD_QUANTITY_IN_CART = "UPDATE cart_item SET quantity = quantity + ? WHERE cart_item_id = (SELECT cart_item_id FROM cart_item WHERE customer_id = ? AND optional_shop_product_item_id = ?)";
    private static final String GET_LIST_OPTIONAL_PRODUCT_ID = "SELECT optional_shop_product_item_id FROM cart_item WHERE customer_id = ?";
    
    public List<CartDTO> getCart(int customer_id) throws SQLException {
        List<CartDTO> list = new ArrayList<>();
        Connection conn = null;
        PreparedStatement ptm = null;
        ResultSet rs = null;
        try {
            conn = DBUtils.getConnection();
            if (conn != null) {
                ptm = conn.prepareStatement(VIEW_CART);
                ptm.setInt(1, customer_id);
                rs = ptm.executeQuery();
                while (rs.next()) {
                    int optional_shop_product_item_id = rs.getInt("optional_shop_product_item_id");
                    int cart_item_id = rs.getInt("cart_item_id");
                    String title = rs.getString("title");
                    Double price = rs.getDouble("price");
                    String name = rs.getString("name");
                    int quantity = rs.getInt("quantity");
                    String category_name = rs.getString("category_name");
                    String image_1 = rs.getString("image_1");
                    int shop_id = rs.getInt("shop_id");
                    String shop_name = rs.getString("shop_name");
                    int inventory = rs.getInt("inventory");
                    
                    
                   
                    list.add(new CartDTO(title,image_1, name, price, quantity, category_name, optional_shop_product_item_id, cart_item_id, shop_id, shop_name, inventory));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (ptm != null) {
                ptm.close();
            }
            if (rs != null) {
                rs.close();
            }
        }
        return list;
    }
public void addToCart(CartDTO addCartItem, List<Integer> listProductInCart) throws SQLException, ClassNotFoundException{
        
        Connection conn = null;
        PreparedStatement ptm = null;
        try {
            conn = DBUtils.getConnection();
            if (conn != null) {
                ptm = conn.prepareStatement(ADD_CART_ITEM);
                for(Integer x: listProductInCart){
                    if(addCartItem.getOptional_shop_product_item_id() == x.intValue()){
                        ptm = conn.prepareStatement(ADD_QUANTITY_IN_CART);
                        break;
                    }
                }
                ptm.setInt(1, addCartItem.getQuantity());
                ptm.setInt(2, addCartItem.getCustomer_id());
                ptm.setInt(3, addCartItem.getOptional_shop_product_item_id());
                ptm.executeUpdate();
               
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (ptm != null) {
                ptm.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
        
    }
public boolean deleteCartItem(int cart_item_id ) throws SQLException{
        boolean checkDelete = false;
        Connection conn = null;
        PreparedStatement ptm = null;
        try{
            conn = DBUtils.getConnection();
            if(conn!= null){
                ptm = conn.prepareStatement(DELETE_CART_ITEM);
                ptm.setInt(1, cart_item_id);
                checkDelete = ptm.executeUpdate()>0?true:false;
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(ptm != null) ptm.close();
            if(ptm != null) conn.close();
            
        }
        return checkDelete;
    }

    public List<Integer> getCartItemId(int customer_id) throws SQLException {
        List<Integer> listID = new ArrayList();
        Connection conn = null;
        PreparedStatement ptm = null;
        ResultSet rs = null;
        try{
            conn = DBUtils.getConnection();
            if (conn != null) {
                ptm = conn.prepareStatement(GET_LIST_OPTIONAL_PRODUCT_ID);
                ptm.setInt(1, customer_id);
                rs = ptm.executeQuery();
                while(rs.next()){
                    int optional_shop_product_item_id = rs.getInt("optional_shop_product_item_id");
                    listID.add(optional_shop_product_item_id);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(rs != null){
                rs.close();
            }
            if (ptm != null) {
                ptm.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
        return listID;
    }
}