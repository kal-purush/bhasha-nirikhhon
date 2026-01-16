import { Request, Response } from 'express';
import fetch from 'node-fetch';
const express = require('express');
const gptDavinciRouter = express.Router();

gptDavinciRouter.post('/davinci',  async  (req: Request, res: Response)  => {
    
    const options = {
        method: "POST",
        headers: {
            "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            model : "text-davinci-003",
            prompt: req.body.prompt,
            max_tokens: req.body.max_tokens,
            n: req.body.n,
            temperature: req.body.temperature,
        })
    }
  

try {
    const response = await fetch('https://api.openai.com/v1/completions', options)
    const data = await response.json()
    res.send(data)
} catch (error) {
    console.error(error)
}
})

export default gptDavinciRouter