import axios from "axios";
import fs from "fs";
import path from "path";

/**
 * https://ressel.se/language/sv/tidtabell-sjostan/?lang=sv
 *
 * - Röda dagar trafikeras som helg
 * - Julafton och nyårsafton trafikeras som helg
 * - Midsommar trafikeras som helg
 */

async function init() {
    const response = await axios.get<Response>(
        "https://sholiday.faboul.se/dagar/v2.1/2021"
    );
    const holidays = response.data.dagar
        .filter(
            (day) =>
                (day["dag i vecka"] !== "6" &&
                    day["dag i vecka"] !== "7" &&
                    day["röd dag"] === "Ja") ||
                day.helgdag === "Midsommarafton" ||
                day.helgdag === "Nyårsafton" ||
                day.helgdag === "Julafton"
        )
        .map((day) => ({
            date: day.datum,
            holiday: day.helgdag,
        }));

    const holidaysPath = path.resolve(__dirname, "../src/services/holidays.ts");
    const holidaysContent = `const holidays = ${JSON.stringify(holidays)};
export default holidays;`;
    fs.writeFileSync(holidaysPath, holidaysContent);
}

init()
    .then(() => console.log("done"))
    .catch((err) => console.error("failed.", err));

export interface Dagar {
    datum: string;
    veckodag: string;
    "arbetsfri dag": string;
    "röd dag": string;
    vecka: string;
    "dag i vecka": string;
    helgdag: string;
    namnsdag: string[];
    flaggdag: string;
    helgdagsafton: string;
    "dag före arbetsfri helgdag": string;
    klämdag: string;
}

export interface Response {
    cachetid: string;
    version: string;
    uri: string;
    startdatum: string;
    slutdatum: string;
    dagar: Dagar[];
}