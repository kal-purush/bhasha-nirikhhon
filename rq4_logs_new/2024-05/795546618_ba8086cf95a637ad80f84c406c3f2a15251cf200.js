"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const cors_1 = __importDefault(require("cors"));
const app = (0, express_1.default)();
app.use((0, cors_1.default)());
app.use(express_1.default.json());
const PORT = 3001;
const questions = [
    {
        id: 0,
        text: "How do you recharge after a busy day?",
        responses: [
            {
                text: "By spending time alone or with a small group of close friends",
                score: 1,
            },
            { text: "By attending social events and being around people", score: 3 },
            { text: "It varies depending on my mood and energy level", score: 2 },
        ],
    },
    {
        id: 1,
        text: "What type of social interaction do you enjoy the most?",
        responses: [
            { text: "Group activities or parties", score: 3 },
            { text: "Casual hangouts with friends", score: 2 },
            {
                text: "I prefer solitary activities over social interaction",
                score: 1,
            },
        ],
    },
    {
        id: 2,
        text: "How do you feel about being the center of attention?",
        responses: [
            { text: "I'm comfortable and enjoy it", score: 3 },
            { text: "It depends on the situation and context", score: 2 },
            { text: "I actively avoid being in the spotlight", score: 1 },
        ],
    },
    {
        id: 3,
        text: "What is your preferred way to spend a weekend?",
        responses: [
            { text: "Engaging in solitary hobbies or activities", score: 1 },
            { text: "Attending social events or gatherings", score: 3 },
            { text: "Balancing both social and alone time", score: 2 },
        ],
    },
];
app.get("/api/questions/count", (req, res) => {
    res.json(questions.length);
});
app.get("/api/questions/:id", (req, res) => {
    const questionId = parseInt(req.params.id);
    const question = questions.find((q) => q.id === questionId);
    if (question) {
        res.json(question);
    }
    else {
        res.status(404).json({ message: "Question not found" });
    }
});
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});