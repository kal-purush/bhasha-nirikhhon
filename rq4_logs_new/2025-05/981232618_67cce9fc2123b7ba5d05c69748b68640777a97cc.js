import express from 'express';
import dotenv from 'dotenv';
import { Configuration, OpenAIApi } from 'openai';
import fs from 'fs';

dotenv.config();
const app = express();
app.use(express.json());

const configuration = new Configuration({
  apiKey: process.env.OPENAI_API_KEY
});
const openai = new OpenAIApi(configuration);

app.post('/ask-gpt', async (req, res) => {
  const { prompt } = req.body;
  if (!prompt) return res.status(400).send({ error: "No prompt provided." });

  try {
    const calendarData = JSON.parse(fs.readFileSync('./calendar_plan.json', 'utf-8'));
    const baseContext = `
You are a helpful planning assistant. Use the user's prompt and the calendar data below to generate a response.

Calendar Plan:
${JSON.stringify(calendarData, null, 2)}
`;

    const completion = await openai.createChatCompletion({
      model: 'gpt-3.5-turbo',
      messages: [
        { role: "system", content: baseContext },
        { role: "user", content: prompt }
      ],
      temperature: 0.7
    });

    res.send({ answer: completion.data.choices[0].message.content });
  } catch (err) {
    console.error("Error talking to OpenAI:", err.message);
    res.status(500).send({ error: "Error communicating with GPT" });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));