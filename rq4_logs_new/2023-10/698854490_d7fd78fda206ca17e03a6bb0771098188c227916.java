/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ulatina.services;

import java.sql.*;
import java.util.*;

/**
 *
 * @author Nwitlyck
 */
public class ServiceDetails extends Service{
    
    public Map<String, Integer> select(int masterId) throws Exception {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, Integer> map = new HashMap<>();

        ps = getConnection().prepareStatement("SELECT Id, Name FROM Details Where Master = ?");
        ps.setInt(1, masterId);
        rs = ps.executeQuery();

        while (rs.next()) {
            
            map.put(rs.getString("Name"), rs.getInt("Id"));
        }

        close(rs);
        close(ps);
        close(conn);

        return map;
    }
    
}