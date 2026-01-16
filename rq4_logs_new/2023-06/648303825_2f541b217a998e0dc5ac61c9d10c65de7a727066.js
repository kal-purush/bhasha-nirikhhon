function verbundspannung() {}

/**
 * Returns rounded number
 * @param {number} decimals
 * @param {number} num number you want to round
 * @returns number
 */
// const round = (decimals, num) => {
//   const roundedFctm =
//     (Math.round((num + Number.EPSILON) * 1 * decimals) / 10) * decimals;
//   return roundedFctm;
// };

const round1decimal = (num) => {
  const roundedFctm = Math.round((num + Number.EPSILON) * 10) / 10;
  return roundedFctm;
};

/**
 * Mittelwert der Zylinderdruckfestigkeit des Betons
 * @param {number} fck charakteristische Zylinderdruckfestigkeit nach 28d
 * @returns number
 */
const fcm = (fck) => {
  return fck + 8;
};

/**
 * Mittelwert der zentrischen Zugfestigkeit des Betons
 * Für fck <= 50  (12 - 30) [N/mm²]
 * @param {number} fck char. Zylinderdruckfestigkeit des Betons nach 28d
 * @returns number
 */
const fctmSmaller50 = (fck) => {
  return 0.3 * fck ** (2 / 3);
};

/**
 * Mittelwert der zentrischen Zugfestigkeit des Betons
 * Für fck > 50 (55 - 100) [N/mm²]
 * @param {number} fck char. Zylinderdruckfestigkeit des Betons nach 28d
 * @returns number
 */
const fctmBigger50 = (fck) => {
  return 2.12 * Math.log(1 + fcm(fck) / 10);
};

/**
 * Entscheidungsfunktion - Mittelwert der zentrischen Zugfestigkeit des Betons
 * Die Funktion ruft in Abhängigkeit von fck die entsprechende
 * Berechnungsfunktion auf.
 * @param {number} fck char. Zylinderdruckfestigkeit des Betons nach 28d
 * @returns number
 */
const fctm = (fck) => {
  if (fck >= 50) {
    return fctmBigger50(fck);
  } else if (fck <= 50) {
    const currFctm = fctmSmaller50(fck);
    return currFctm;
  }
};

/**
 * unterercharakteristischer Grenzwert der zentrischen Betonzugfestigkeit
 * @param {number} fctm Mittelwert der zentrischen Zugfestigkeit des Betons
 * @returns number
 */
const fctk005 = (fctm) => {
  return 0.7 * fctm;
};

// const fbd = (fctk005, verbundBedingung) => {

//     if (verbundBedingung ==="guterVerbund") {
//         const eta1
//     } if (verbundBedingung ==="schlechterVerbund") {
//         const eta1
//     }

// };

export { fctm, fctk005, fctmSmaller50, fctmBigger50, round1decimal };