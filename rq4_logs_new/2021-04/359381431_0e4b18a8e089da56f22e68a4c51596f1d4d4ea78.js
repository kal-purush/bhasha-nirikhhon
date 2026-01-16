let bayes = require('bayes')
const fs = require('fs')

const negativeBase = fs.readFileSync("negativeBase.txt", "utf8").toLowerCase()
const positiveBase = fs.readFileSync("positiveBase.txt", "utf8").toLowerCase()
const positiveCategory = fs.readFileSync("positiveCategory.txt", "utf8").toLowerCase()
const negativeCategory = fs.readFileSync("negativeCategory.txt", "utf8").toLowerCase()

let classifier = bayes()

const classifierLearn = async (str, category) => {
    try {
        let arr = str.split(' ')
        console.log(arr.length)
        for(let index in arr) {
            await classifier.learn(arr[+index], category)
        }
    } catch (e) {}
}

classifierLearn(positiveBase, 'positive').then(() => {
    console.log('DONE positive learn')
    classifierLearn(negativeBase, 'negative').then(()=>{
        console.log('DONE negative learn')
        classifier.categorize(positiveCategory).then((r) => {
            console.log(`positive categorize: ${r}`)
        })
        classifier.categorize(negativeCategory).then((r) => {
            console.log(`negative categorize: ${r}`)
        })
    })
})

