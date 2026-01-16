document.addEventListener("DOMContentLoaded", () => {
  const answers = document.querySelectorAll(".answer");
  const result = document.querySelector(".result");
  const resultText = document.querySelector(".result-text");
  const cta = document.querySelector(".cta"); // Call-to-Action-Element

  if (!answers.length || !result || !resultText || !cta) {
    console.error("Fehlende HTML-Elemente: Bitte überprüfe die HTML-Struktur.");
    return;
  }

  answers.forEach(answer => {
    answer.addEventListener("click", () => {
      const selectedAnswer = answer.getAttribute("data-answer");
      let message;

      // Logik für die Antworten
      if (selectedAnswer === "A") {
        message =
          "Du bist ein Flow-Liebhaber! Mit KI arbeitest Du leicht und effizient, ohne die Kontrolle zu verlieren.";
        cta.classList.remove("hidden"); // CTA sichtbar machen
      } else if (selectedAnswer === "B") {
        message =
          "Verantwortung und System sind Deine Stärken. Mit KI setzt Du auf klare Prozesse, um Ziele zu erreichen.";
        cta.classList.remove("hidden"); // CTA sichtbar machen
      } else if (selectedAnswer === "C") {
        message =
          "Kreativität steht für Dich im Vordergrund. Mit Bend, Break, Build formst Du innovative Ideen.";
        cta.classList.remove("hidden"); // CTA sichtbar machen
      } else if (selectedAnswer === "D") {
        message =
          "Es ist okay, wenn Du Verantwortung im digitalen Raum (noch) nicht übernehmen willst. Jeder Schritt braucht seinen Moment.";
        cta.classList.add("hidden"); // CTA versteckt halten
      }

      resultText.textContent = message;
      result.classList.remove("hidden"); // Ergebnis sichtbar machen
    });
  });
});