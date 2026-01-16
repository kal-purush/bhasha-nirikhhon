import * as cheerio from 'cheerio';
import fetch from 'node-fetch'
import FormData from 'form-data';

main();

async function getConjunctions() {
    try {
        var requestOptions = {
            method: 'GET',
            redirect: 'follow'
        };

        const load = await fetch("http://celestrak.org/SOCRATES/search-results.php?IDENT=CATNR&CATNR_TEXT1=25544&ORDER=MINRANGE&MAX=25&B1=Submit", requestOptions)
        const loadedData = await load.text();
        const $ = cheerio.load(loadedData)
        const table = $('table.center.outline').text().replaceAll(" ", "").replace(/\s+/g, ' ').trim().split(" ");

        var array = []
        var initial = 11
        for (let item = 0; item < ((table.length - 11) / 13); item++) {
            var arr = []
            for (let i = 0; i < 13; i++) {
                arr.push(table[initial + i])
            }

            var issName = arr[1].match(/(.*).{3}$/)[1];
            var objName = arr[8].match(/(.*).{3}$/)[1];

            async function getTLE(formData) {
                try {
                    const res = await fetch("http://celestrak.org/SOCRATES/conjunction.php", {
                        "headers": {
                            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                            "accept-language": "en-US,en;q=0.9",
                            "cache-control": "max-age=0",
                            "content-type": "application/x-www-form-urlencoded",
                            "upgrade-insecure-requests": "1",
                            "cookie": "PHPSESSID=5mp06bmcon01pshu299asmi77s",
                            "Referrer-Policy": "strict-origin-when-cross-origin",
                            ...formData.getHeaders()
                        },
                        "body": formData,
                        "method": "POST"
                    });

                    const tleDataTemp = await res.text();
                    const tleData = tleDataTemp.replaceAll(" ", "").replace(/\s+/g, ' ').trim().split(" ");
                    var tle = {
                        "iss": `${tleData[11]} ${tleData[12]} ${tleData[13]}`,
                        "object": `${tleData[14]} ${tleData[15]} ${tleData[16]}`
                    }
                    return tle
                } catch (error) {
                    console.log(error)
                }
            }

            let data = new FormData();
            data.append('ACTION', 'TLE Data');
            data.append('CATNR1', arr[0]);
            data.append('NAME1', issName);
            data.append('RVEL', arr[6]);
            data.append('CATNR2', arr[7]);
            data.append('NAME2', objName);
            data.append('T0', arr[10]);
            data.append('T1', arr[11]);
            data.append('T2', arr[12]);

            const tleData = await getTLE(data);

            var obj = {
                "conjunction": {
                    "probability": JSON.parse(arr[3]),
                    "dilutionThreshold": arr[4],
                    "minRange": arr[5],
                    "velocity": arr[6],
                    "start": arr[10],
                    "tca": arr[11],
                    "stop": arr[12]
                },
                "iss": {
                    "noradId": arr[0],
                    "name": issName,
                    "daysSinceEpoch": arr[2],
                    'tle': tleData.iss
                },
                "object": {
                    "noradId": arr[7],
                    "name": objName,
                    "daysSinceEpoch": arr[9],
                    'tle': tleData.object
                }
            }
            initial = initial + 13
            array.push(obj)
        }

        return array
    } catch (error) {
        console.log(error)
    }
}

async function main() {
    const TLE_object = await getConjunctions();
    console.log(TLE_object)
}