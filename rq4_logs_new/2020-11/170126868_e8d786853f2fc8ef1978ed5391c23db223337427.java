package no.nav.fo.veilarb.dokument.domain;

import lombok.Value;

@Value
public class EnhetPostboksadresse extends EnhetPostadresse {
    String postnummer;
    String poststed;
    String postboksnummer;
    String postboksanlegg;
}