import { Request, Response } from "express";

import { GoogleGenerativeAI } from "@google/generative-ai";

let projectPrompt =
    "I am writing a project post for a freelance platform. I am looking to hire a freelancer for my project. You will give me suggestion about what to write next in my description. Just give me plain text and only include what should be written next. ";

const getAiSuggestions = async (req: Request, res: Response) => {
    const { title, budget, type, description, deadline } = req.query;

    try {
        if (!process.env.GEMINI_API_KEY) {
            process.exit(1);
        }

        const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
        const model = genAI.getGenerativeModel({ model: "gemini-1.5-flash" });

        if (title) projectPrompt += `This is my project title: ${title}`;
        if (budget) projectPrompt += `This is my project budget: ${budget}`;
        if (deadline) projectPrompt += `This is my project deadline: ${deadline}`;
        if (description)
            projectPrompt += `This is the description I've written till now: ${description}`;

        const result = await model.generateContent(projectPrompt);
        const suggestion = result.response.text();
        console.log(suggestion);

        res.status(200).json({ suggestion, error: false });
    } catch (e: any) {
        console.log("Failed to get suggestion", e);
        res.status(500).json({ message: e.message || "An error has occured.", error: true });
    }
};

export default getAiSuggestions;