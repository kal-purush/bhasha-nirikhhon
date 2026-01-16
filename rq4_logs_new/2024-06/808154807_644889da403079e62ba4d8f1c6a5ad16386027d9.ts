import { GoogleGenerativeAI } from "@google/generative-ai";


const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const model = genAI.getGenerativeModel({ model: "gemini-1.5-flash" });

async function run() {
    const prompt = "Escreva uma história de background para o jogo Dungeon and Dragons usando o personagem da raça bárbara usando 200 caracteres"
  
    const result = await model.generateContent(prompt);
    const response = result.response;
    const text = response.text();
    console.log(text);
  }