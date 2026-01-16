

// const openai = new OpenAI({
//     apiKey: process.env.OPENAI_API_KEY,
// });

const summarize = async (text: string, openAiInstance: any) => {

    const prompt = `
    Transcribed Text:
    ${text}
    Convert the above transcribed text into key points and main ideas using Markdown, which splits 
    words and text into headers/headings, bullet points, bolding, italics, underlines, etc. 
    (and any combination thereof)? Ensure YOU DO NOT deviate from this style format for every 
    message given to you. Sometimes, the message will not be long enough, and you may need to wait 
    a bit before processing the file. DO NOT just convert text to markdown. Highlight what is 
    important information to take out from the provided conversation using a traditional style guide. 
    Create it as if you were writing detailed notes with important examples. Do not miss out on 
    information. Ensure the generated text includes relevant details about the topic discussed. 
    Please additionally add a summary at the end or a conclusion. Adapt the response to the context 
    of the conversation, including concepts, examples, and any recommended style guide. Output 
    generated markdown as a code block. Do not allow the generated text to fall outside the code block.    
    `;

    const response = await openAiInstance.completions.create({
        model: "gpt-3.5-turbo-instruct",
        prompt: prompt,
        max_tokens: 500,
    }).then((res) => {
        console.log(res.choices[0].text);
        return res;
    }).catch((err) => { console.error(err); });
    return response;
}

export default summarize;