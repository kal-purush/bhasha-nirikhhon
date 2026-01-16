package nokia;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Resource;
import javax.enterprise.context.RequestScoped;
import javax.inject.Named;
import javax.sql.DataSource;

@Named
@RequestScoped
public class NokianKurssit {
    private Date alkupäivä;
    private Date loppupäivä;
    
    @Resource(lookup="jdbc/07-nokia")
    private DataSource db;

    public String getAlkupäivä() {
        return alkupäivä!=null?alkupäivä.toString():null;
    }

    public void setAlkupäivä(String alkupäivä) {
        this.alkupäivä = Date.valueOf(alkupäivä);
    }

    public String getLoppupäivä() {
        return loppupäivä!=null?loppupäivä.toString():null;
    }

    public void setLoppupäivä(String loppupäivä) {
        this.loppupäivä = Date.valueOf(loppupäivä);
    }
    
    public List<Kurssi> getKurssit() throws SQLException {
        List<Kurssi> kurssit = new LinkedList<>();
        
        System.out.println(alkupäivä + " - " + loppupäivä);
        
        try (
            Connection c = db.getConnection();
            PreparedStatement ps = c.prepareStatement(
                "select tradedate,closing_price from kurssit where"
                        + " tradedate between ? and ?"
                );
            ) {
            
            ps.setDate(1, alkupäivä);
            ps.setDate(2, loppupäivä);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                kurssit.add(new Kurssi(
                    rs.getDate("TRADEDATE"),
                    rs.getBigDecimal("CLOSING_PRICE")
                ));
            }
        }
        
        return kurssit;
    }

    public static class Kurssi {
        private Date päivä;
        private BigDecimal loppukurssi;
        
        public Kurssi() {
        }

        public Kurssi(Date päivä, BigDecimal loppukurssi) {
            this.päivä = päivä;
            this.loppukurssi = loppukurssi;
        }

        public String getPäivä() {
            return päivä.toString();
        }

        public String getLoppukurssi() {
            return loppukurssi.toString();
        }    
    }
}