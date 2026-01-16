const {MessageEmbed } = require("discord.js")

const ballChoices = [
   {name: "Yes", value: "Yes"},
   {name: "No", value: "No"}
]

const get8Ball = async (interaction, question, biased, lucky) => {

    let biasedText = ""
    if (biased === "Yes") {
        biasedText = "biased"
    }

    let luckyText = "false"
    if (lucky === "Yes") {
        luckyText = "true"
    }
    let url = `https://eightballapi.com/api/${biasedText}?question=${question}&lucky=${luckyText}`
    console.log(url)
    let request = await fetch(url)
    let response = await request.json()
    //console.log(response)
    return response
}

const run = async (client, interaction) => {
    let question = interaction.options.getString("question")
    let biased = interaction.options.getString("biased")
    let lucky = interaction.options.getString("lucky")

    if (!question) return interaction.reply("Invalid question")
    question = question.replaceAll(" ", "+")
    try {
        const ballData = await get8Ball(interaction, question, biased, lucky)
        if (!interaction.replied && ballData) {
            return interaction.reply(ballData.reading)
        }
    }
    catch(err){
        if (err){
            console.error(err)
            if (!interaction.replied) {
                return interaction.reply(`Failed to retrieve your daily horoscope!`)
            }
        }
    }
}

module.exports = {
    name: "8ball",
    description: "Ask 8ball anything!",
    options: [
        {
            name: "question",
            description: "Ask 8Ball a question!",
            type: 3, //STRING
            required: true
        },
        {
            name: "biased",
            description: "Reading will use basic sentiment analysis to create a biased reading based on the question asked.",
            type: 3, //STRING
            choices: ballChoices,
            required: false
        },
        {
            name: "lucky",
            description: "Reading will try to give favorable response based on the sentiment of question asked.",
            type: 3, //STRING
            choices: ballChoices,
            required: false
        },
    ], 
    run
}