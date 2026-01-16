// Kjør når hele HTML-dokumentet er ferdig lastet
document.addEventListener("DOMContentLoaded", () => {
  // Hent skjemaet med ID "bedriftForm"
  const form = document.getElementById("bedriftForm");

  // Definer hvilke felt som skal valideres, med regex og feilmeldinger
  const felter = [
    { name: "orgnr", regex: /^\d{9}$/, requiredMsg: "Fyll inn organisasjonsnummer", patternMsg: "Må være 9 sifre" },
    { name: "bedriftsnavn", requiredMsg: "Fyll inn navn på bedriften" },
    { name: "adresse1", requiredMsg: "Fyll inn adresse" },
    { name: "postnr", regex: /^\d{4}$/, requiredMsg: "Fyll inn postnummer", patternMsg: "Må være 4 sifre" },
    { name: "sted", requiredMsg: "Fyll inn sted" },
    { name: "epost", regex: /^[^\s@]+@[^\s@]+\.[^\s@]+$/, requiredMsg: "Fyll inn e-post", patternMsg: "Ugyldig e-post" },
    { name: "kontaktperson", requiredMsg: "Fyll inn kontaktperson" },
    { name: "kontaktpersonTlf", regex: /^\d{8}$/, requiredMsg: "Fyll inn kontaktpersonens telefonnummer", patternMsg: "Må være 8 sifre" }
  ];

  // Gå gjennom hvert felt og legg til validering
  felter.forEach(({ name, regex, requiredMsg, patternMsg }) => {
    const input = document.querySelector(`input[name="${name}"]`);         // Finn input-feltet
    const error = document.getElementById(`${name}Error`);                 // Finn tilhørende feilmeldings-div
    if (!input || !error) return;                                          // Hopp over hvis felt eller feilmelding mangler

    // Funksjon som viser riktig feilmelding basert på input
    const visFeil = () => {
      const verdi = input.value.trim();                                   // Fjern mellomrom
      error.textContent = "";                                             // Tøm tidligere feilmelding

      if (!verdi) {
        error.textContent = requiredMsg;                                  // Hvis tomt → vis påkrevd-melding
      } else if (regex && !regex.test(verdi)) {
        error.textContent = patternMsg;                                   // Hvis regex ikke matcher → vis mønsterfeil
      }
    };

    input.addEventListener("input", visFeil);                             // Kjør validering ved skriving
    input.addEventListener("blur", visFeil);                              // Kjør validering når man forlater feltet
  });

  // Når skjemaet forsøkes sendt inn
  form.addEventListener("submit", (e) => {
    e.preventDefault();                                                   // Hindre innsending hvis noe er ugyldig
    let ugyldig = false;

    // Gå gjennom alle felt og valider igjen før innsending
    felter.forEach(({ name, regex, requiredMsg, patternMsg }) => {
      const input = document.querySelector(`input[name="${name}"]`);
      const error = document.getElementById(`${name}Error`);
      const verdi = input.value.trim();

      error.textContent = "";                                             // Nullstill feilmelding

      if (!verdi) {
        error.textContent = requiredMsg;                                  // Felt mangler verdi
        ugyldig = true;
      } else if (regex && !regex.test(verdi)) {
        error.textContent = patternMsg;                                   // Regex validering feilet
        ugyldig = true;
      }
    });

    if (!ugyldig) {
      form.submit();                                                      // Alt gyldig → send skjema
    }
  });
});