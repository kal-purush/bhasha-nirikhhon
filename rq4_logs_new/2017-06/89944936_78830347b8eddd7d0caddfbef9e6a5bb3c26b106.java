package pkg07.nokia;

import java.io.File;
import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

public class Nokia {
    private static final String fileName = 
            "NOK1V-2000-01-01-2014-12-31.csv";
    
    private final Scanner fs;
    private String kenttäerotin = ",";
    private String seuraavaRivi;
    private String[] otsikot;
    private String vientilause;
    private final Connection db;
    
    private Nokia() throws FileNotFoundException, SQLException {
        fs = new Scanner(new File(fileName));
        haeSeuraavaRivi();
        db = DriverManager.getConnection(
                "jdbc:derby://localhost:1527/07-nokia", 
                "timo", 
                "timo");
    }
    
    private void haeSeuraavaRivi() {
        if (fs.hasNextLine()) {
            seuraavaRivi = fs.nextLine();
        } else {
            seuraavaRivi = null;
        }
    }
    
    private void lueSep() {
        if (seuraavaRivi == null) return;
        if (seuraavaRivi.startsWith("sep=")) {
            kenttäerotin = seuraavaRivi.substring(4);
            haeSeuraavaRivi();
            System.out.println("Kenttäerotin muutetti "+kenttäerotin);
        }
    }
    
    private void lueOtsikot() throws SQLException {
        if (seuraavaRivi == null) return;
        
        otsikot = seuraavaRivi.split(kenttäerotin);
        System.out.println("Otsikot: " + String.join(" ", otsikot));
        for (int i = 0; i < otsikot.length; ++i) {
            otsikot[i] = otsikot[i].toLowerCase().replace(" ", "_");
            if (otsikot[i].equals("date"))
                otsikot[i] = "tradedate";
        }
        haeSeuraavaRivi();
        
        // Poistetan vanha taulu
        ResultSet taulut = 
                db.getMetaData().getTables(null, null, "KURSSIT", null);
        if (taulut.next()) {
            System.out.println(taulut.getString("TABLE_NAME"));
            db.createStatement().execute("drop table kurssit");
        } else {
            System.out.println("Ei taulua nimeltä \"kurssit\"");
        }
        
        // Luodaan uusi taulu ja vientilause
        StringBuilder luontilause = new StringBuilder();
        StringBuilder vientilause = new StringBuilder();
        
        luontilause.append("create table kurssit (");
        vientilause.append("insert into kurssit values (");
        
        for (String sarake : otsikot) {
            if (sarake.equals("tradedate")) {
                luontilause.append(sarake).append(" date primary key");
                vientilause.append("?");
            } else if (!sarake.isEmpty()) { 
                luontilause.append(" , ")
                    .append(sarake)
                    .append(" decimal(16,4)");
                vientilause.append(",?");
            }
        }
        luontilause.append(")");
        vientilause.append(")");
        
        this.vientilause = vientilause.toString();
        db.createStatement().execute(luontilause.toString());
        
        System.out.println(luontilause);
        System.out.println(vientilause);
    }
    
    private void tallennaKurssit() throws SQLException {
        PreparedStatement ps = db.prepareStatement(vientilause);
        
        while (seuraavaRivi != null) {
            // Tallennetaan rivi
            String kentät[] = seuraavaRivi.split(kenttäerotin);
            ps.setDate(1, Date.valueOf(kentät[0]));
            try {
                for (int i = 1; i < kentät.length; ++i) {
                    kentät[i] = kentät[i].replace(",", ".");
                    if (kentät[i].isEmpty()) {
                        ps.setBigDecimal(i+1, null);
                    } else {
                        ps.setBigDecimal(i+1, new BigDecimal(kentät[i]));
                    }
                }
            } catch ( Exception ex ) {
                ex.printStackTrace();
            }
            ps.execute();
            haeSeuraavaRivi();
        }
    }
    
    public static void main(String[] args) throws Exception {
        Nokia nokia = new Nokia();
        nokia.lueSep();
        nokia.lueOtsikot();
        nokia.tallennaKurssit();
    }
    
}