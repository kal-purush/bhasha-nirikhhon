import { Incident } from './types';

export const initialIncidents: Incident[] = [
  {
    id: 1,
    title: "Biased Recommendation Algorithm",
    description: "Algorithm consistently favored certain demographics in product recommendations, leading to unfair treatment of user segments. This was discovered during a routine audit of the recommendation system's output patterns.",
    severity: "Medium",
    reported_at: "2025-03-15T10:00:00Z"
  },
  {
    id: 2,
    title: "LLM Hallucination in Critical Info",
    description: "LLM provided incorrect safety procedure information during an emergency response simulation, highlighting the need for better fact-checking mechanisms and human oversight in critical applications.",
    severity: "High",
    reported_at: "2025-04-01T14:30:00Z"
  },
  {
    id: 3,
    title: "Minor Data Leak via Chatbot",
    description: "Chatbot inadvertently exposed non-sensitive user metadata during conversations. While no critical information was compromised, this incident highlighted the need for improved data handling protocols.",
    severity: "Low",
    reported_at: "2025-03-20T09:15:00Z"
  }
];