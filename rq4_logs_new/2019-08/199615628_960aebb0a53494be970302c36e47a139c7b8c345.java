package com.gemeenteutrecht.processplatform.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public interface Zaak {

    String identificatie();

    String bronorganisatie();

    String omschrijving();

    String toelichting();

    URI zaaktype();

    LocalDate registratiedatum();

    String verantwoordelijkeOrganisatie();

    LocalDate startdatum();

    LocalDate einddatumGepland();

    LocalDate uiterlijkeEinddatumAfdoening();

    LocalDate publicatiedatum();

    URI communicatiekanaal();

    List<URI> productenOfDiensten();

    String vertrouwelijkheidaanduiding();

    String betalingsindicatie();

    LocalDateTime laatsteBetaaldatum();

    @JsonProperty("zaakgeometrie")
    Geometrie geometrie();

    Verlenging verlenging();

    Opschorting opschorting();

    URI selectielijstklasse();

    URI hoofdzaak();

    List<URI> relevanteAndereZaken();

    List<Kenmerk> kenmerken();

    @JsonProperty("archiefnominatie")
    ArchiefNominatie archiefNominatie();

    @JsonProperty("archiefstatus")
    ArchiefStatus archiefStatus();

    @JsonProperty("archiefactiedatum")
    LocalDate archiefActieDatum();
}