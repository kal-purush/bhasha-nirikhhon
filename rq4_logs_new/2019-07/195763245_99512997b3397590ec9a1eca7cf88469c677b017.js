const csv = require('csv-parser')
const fs = require('fs')

const results = [];
let distribution = new Array(52).fill(0)
let contribution = new Array(52).fill(0)

let file = process.argv[2];
console.log(file)

fs.createReadStream(file)
    .pipe(csv())
    .on('data',
        (data) => {
            let amount = parseInt(data.contribution_receipt_amount)
            if (amount > 0 && amount <= 5000) {
                let amt = parseInt(data.contribution_receipt_amount)
                let cohort = (Math.floor(amt / 100))
                distribution[cohort] += 1;
                contribution[cohort] += amt;
            } else if (amount > 5000) {
                distribution[51] += 1;
                contribution[51] += amount;
            }

            //results.push(data)
        })
    .on('end',
        () => {
            {
                let total = 0;
                let freqTotal = 0
                for (let i = 0; i < distribution.length - 1; i++) {
                    total += contribution[i]
                    freqTotal += distribution[i]
                }


                console.log('Cohort\t\tFreq\t\tContribution Tot\tContribution%\t\tFrequency%\t\ttotal=' + total)
                for (let i = 0; i < distribution.length; i++) {
                    let dist = distribution[i];
                    let cont = contribution[i];
                    let contShare = cont / total;
                    let freqShare = dist / freqTotal;
                    if (i < distribution.length - 1)
                        console.log(i + '\t\t' + dist + '\t\t' + cont + '\t\t\t' + (contShare * 100).toFixed(2) + '\t\t\t' + (freqShare * 100).toFixed(2))
                }
            }
        })