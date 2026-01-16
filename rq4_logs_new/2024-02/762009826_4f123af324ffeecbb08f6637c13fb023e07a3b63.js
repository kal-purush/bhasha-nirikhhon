//make sure npm i express openai
const OpenAI = require("openai");
const readline = require("readline");

const openai = new OpenAI({
    apiKey: "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"
});

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

let chatHistory = [];

async function generateResponse() {
    // Take user input for the prompt
    rl.question("Enter a prompt (N to exit): ", async (prompt) => {
        if (prompt.toUpperCase() === 'N') {
            rl.close();
            return;
        }

        chatHistory.push({ "role": "user", "content": prompt }); // adding user input to history

        try {
            const response = await openai.chat.completions.create({
                model: 'gpt-3.5-turbo',
                messages: chatHistory,
                max_tokens: 100 // remove/change if you want a longer response (1 token around 4 characters) 
            });

            //bot response
            console.log(response.choices[0].message.content);

            // adding bot response to history
            chatHistory.push({ "role": "assistant", "content": response.choices[0].message.content });

            generateResponse();
        } catch (error) {
            console.error(error);
            generateResponse();
        }
    });
}

generateResponse();