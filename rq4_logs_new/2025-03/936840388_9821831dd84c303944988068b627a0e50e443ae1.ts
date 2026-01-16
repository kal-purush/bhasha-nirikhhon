import { Request, Response } from "express";
import aiService from "../services/aiService";

export const generatePostContent = async (req: Request, res: Response) => {
  try {
    const { title, topic } = req.body;

    // Create a prompt based on the post title and topic
    let prompt = `Write an engaging blog post about "${title}".`;

    if (topic && topic.trim()) {
      prompt += ` Focus on ${topic}.`;
    }

    prompt += " The content should be informative and well-structured.";

    const generatedContent = await aiService.generateContent(prompt);

    res.status(200).json({ content: generatedContent });
  } catch (error) {
    console.error("Error in generatePostContent:", error);
    res.status(500).json({ error: "Failed to generate content" });
  }
};