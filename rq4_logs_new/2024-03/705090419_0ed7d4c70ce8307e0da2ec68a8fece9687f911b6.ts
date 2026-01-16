import OpenAI from "openai";
import * as dotenv from "dotenv";

dotenv.config();

const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
    dangerouslyAllowBrowser: true
});

let firstString: string = "";
let transcriptionIteration: number = 0;
const transcribe = async (audio: Blob) => {
    /* Function that will use OpenAI's Whisper API to transcribe audio. */

    const file = new File([audio], "audio.wav", { "type": 'audio/wav; codecs=1' });
    const transcription = await openai.audio.transcriptions.create({
        file: file,
        model: "whisper-1",
    }).then((res) => {
        if (transcriptionIteration === 0) {
            firstString = res.text;
            transcriptionIteration++;
        }
        console.log(res.text.slice(firstString.length, res.text.length).trim());

        return res;
    }).catch((err) => { console.error(err); });
    return transcription;
}

export default transcribe;