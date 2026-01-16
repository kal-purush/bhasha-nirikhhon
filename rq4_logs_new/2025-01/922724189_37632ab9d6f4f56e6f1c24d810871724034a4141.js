require('dotenv').config();  // Load environment variables from .env
const express = require('express');
const { OpenAI } = require('openai');  // Import OpenAI library
const cors=require('cors');

const app = express();
const port = 3000;
app.use(cors());

// Initialize OpenAI client with API key from environment variables
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

const deepseek = new OpenAI({
    baseURL: 'https://api.deepseek.com',
    apiKey: process.env.DEEPSEEK_API_KEY,
});

// Middleware to parse JSON requests
app.use(express.json());

// Endpoint to receive a prompt and respond with OpenAI's chat response
app.post('/message-openai', async (req, res) => {
  const { prompt } = req.body;

  if (!prompt) {
    return res.status(400).json({ error: 'Prompt is required' });
  }

  try {
    // Make a call to OpenAI's chat API with the provided prompt
    const chatCompletion = await openai.chat.completions.create({
      messages: [{ role: 'user', content: prompt }],
      model: 'o1-mini-2024-09-12', // Specify model to use
    });

    // Send back the response from OpenAI
    res.json({
      response: chatCompletion.choices[0].message.content,
    });
  } catch (error) {
    console.error('Error with OpenAI API:', error);
    res.status(500).json({ error: 'An error occurred while processing your request' });
  }
});

app.post('/message-deepseek', async (req, res) => {
    const { prompt } = req.body;
  
    if (!prompt) {
      return res.status(400).json({ error: 'Prompt is required' });
    }
  
    try {
      // Make a call to OpenAI's chat API with the provided prompt
      const chatCompletion = await deepseek.chat.completions.create({
        messages: [{ role: 'user', content: prompt }],
        model: 'deepseek-reasoner', // Specify model to use
      });
  
      // Send back the response from OpenAI
      res.json({
        response: chatCompletion.choices[0].message.content,
      });
    } catch (error) {
      console.error('Error with OpenAI API:', error);
      res.status(500).json({ error: 'An error occurred while processing your request' });
    }
  });

// Start the Express server
app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
});