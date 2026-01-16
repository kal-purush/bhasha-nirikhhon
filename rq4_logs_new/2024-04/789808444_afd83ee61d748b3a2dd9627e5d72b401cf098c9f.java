package oslomet.data1700_oblig_3;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor


public class Billett {

    private Long billettNr;
    private String film;
    private Integer antall;
    private String fornavn;
    private String etternavn;
    private String telefon;
    private String epost;

    public Billett(String film, Integer antall, String fornavn, String etternavn, String telefon, String epost){
        this.film = film;
        this.antall = antall;
        this.fornavn = fornavn;
        this.etternavn =etternavn;
        this.telefon = telefon;
        this.epost = epost;
    }
}