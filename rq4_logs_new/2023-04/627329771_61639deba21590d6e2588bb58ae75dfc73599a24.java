package org.flowershop.service;
import java.sql.*;

public class QueryMySQL {

    public static int getTotalStock(Connection conn) throws SQLException {
        String query = "SELECT COUNT(*) AS total_stock FROM product";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        int totalStock = 0;
        if (rs.next()) {
            totalStock = rs.getInt("total_stock");
        }
        rs.close();
        stmt.close();
        return totalStock;
    }

    public static double getTotalValue(Connection conn) throws SQLException {
        String query = "SELECT SUM(price * stock) AS total_value FROM product";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        double totalValue = 0;
        if (rs.next()) {
            totalValue = rs.getDouble("total_value");
        }
        rs.close();
        stmt.close();
        return totalValue;
    }

    public static void printTicketsBetweenDates(Connection conn, Date fromDate, Date toDate) throws SQLException {
        String query = "SELECT * FROM Ticket WHERE date BETWEEN ? AND ?";
        PreparedStatement pstmt = conn.prepareStatement(query);
        pstmt.setDate(1, fromDate);
        pstmt.setDate(2, toDate);
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            int id = rs.getInt("id");
            Date date = rs.getDate("date");
            double totalPrice = rs.getDouble("total_price");
            System.out.println("Ticket " + id + " del " + date + " con precio total de " + totalPrice);
        }
        rs.close();
        pstmt.close();
    }

    public static double getTotalProfitBetweenDates(Connection conn, Date fromDate, Date toDate) throws SQLException {
        String query = "SELECT SUM(total_price) AS total_profit FROM Ticket WHERE date BETWEEN ? AND ?";
        PreparedStatement pstmt = conn.prepareStatement(query);
        pstmt.setDate(1, fromDate);
        pstmt.setDate(2, toDate);
        ResultSet rs = pstmt.executeQuery();
        double totalProfit = 0;
        if (rs.next()) {
            totalProfit = rs.getDouble("total_profit");
        }
        rs.close();
        pstmt.close();
        return totalProfit;
    }
}