package model;

import com.j256.ormlite.dao.ForeignCollection;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.ForeignCollectionField;

import java.util.Collection;

/**
 * Created by androiddevelopment on 20.1.17..
 */
public class Brod {


    public static final String POLJE_OZNAKA = "oznaka";
    public static final String POLJE_NAZIV = "naziv";

    @DatabaseField(generatedId = true)
    private int id;
    @DatabaseField(columnName = POLJE_OZNAKA, canBeNull = false)
    private String oznaka;

    @DatabaseField(columnName = POLJE_NAZIV, canBeNull = false)
    private double naziv;

    @ForeignCollectionField(foreignFieldName = "brod")
    private ForeignCollection<Kontejner> kontejner;

    public Brod(String brod1, String transporter1) {
    }

    public Brod(String oznaka, double oznaka) {
        this.oznaka = oznaka;
        this.oznaka = oznaka;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getNaziv() {
        return naziv;
    }

    public void setNaziv(String naziv) {
        this.naziv = naziv;
    }

    public double getOznaka() {
        return oznaka;
    }

    public void setOznaka(double oznaka) {
        this.oznaka = oznaka;
    }

    public Collection<kontejner> getKontejner() {
        return kontejner;
    }

    public void setKontejner(ForeignCollection<Kontejner> roba) {
        this.kontejner = kontejner;
    }

    public String toString() {
        return "Brod{" +
                "id=" + id +
                ", oznaka='" + oznaka + '\'' +
                ", naziv=" + naziv +
                '}';
    }
}


}