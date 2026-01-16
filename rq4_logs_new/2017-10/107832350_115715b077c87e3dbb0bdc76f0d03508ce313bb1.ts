import * as request from "request";

export = function simpleHttpRequest(hook: any) {
    request.get("http://www.bom.gov.au/fwo/IDV60901/IDV60901.94870.json", (err, res, body) => {
        if (err) {
            return hook.res.end(err.messsage);
        }
        const json = JSON.parse(body);
        const datum = json.observations.data[0];
        const airTemp = datum.air_temp;
        const relativeHumidity = datum.rel_hum;
        const airPressure = datum.press_msl;
        const evapCoolingEfficiency = 0.7;

        const wetBulb = calculateWetBulb(airTemp, relativeHumidity, airPressure);
        const airTempToWetBulbDelta = airTemp - wetBulb;
        const maxEvapCoolingTempDrop = airTempToWetBulbDelta * evapCoolingEfficiency;
        const minPossTemp = airTemp - maxEvapCoolingTempDrop;

        hook.res.end("<html><body><h1>" + minPossTemp.toFixed(1) + "</h1></body></html>");
    });
};

function calculateWetBulb(Ctemp: any, rh: any, MBpressure: any) {
    const rhFloat = parseFloat(rh);

    const Es = esubs(Ctemp);

    const E2 = invertedRH(Es, rhFloat);

    return calcwetbulb(Ctemp, MBpressure, E2);
}

function esubs(Ctemp: any) {
    const Es = 6.112 * Math.exp(17.67 * Ctemp / (Ctemp + 243.5));
    return Es;
}

function invertedRH(Es: any, rh: any) {
    const E = Es * (rh / 100);
    return E;
}

function calcwetbulb(Ctemp: any, MBpressure: any, E2: any) {
    let Twguess = 0;
    let incr = 10;
    let previoussign = 1;
    let Edifference = 1;

    while (Math.abs(Edifference) > 0.05) {
        const Ewguess = 6.112 * Math.exp((17.67 * Twguess) / (Twguess + 243.5));
        const Eguess = Ewguess - MBpressure * (Ctemp - Twguess) * 0.00066 * (1 + (0.00115 * Twguess));
        Edifference = E2 - Eguess;

        if (Edifference === 0) {
            break;
        } else {
            if (Edifference < 0) {
                reverseDirectionIfNeeded(-1);
            } else {
                reverseDirectionIfNeeded(1);
            }
        }

        Twguess = Twguess + incr * previoussign;
    }

    return Twguess;

    function reverseDirectionIfNeeded(cursign: any) {
        if (cursign !== previoussign) {
            //Overshoot - reverse direction and decrease step size
            previoussign = cursign;
            incr /= 10;
        }
    }
}