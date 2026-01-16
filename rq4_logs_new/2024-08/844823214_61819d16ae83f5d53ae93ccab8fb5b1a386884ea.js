const fastify = require('fastify')({ logger: true });
const fs = require('fs').promises;
const PDFParser = require('pdf-parse');
const axios = require('axios');

const PORT = process.env.PORT || 3000;
const CLAUDE_API_URL = 'https://api.anthropic.com/v1/messages';
const CLAUDE_MODEL = 'claude-3-opus-20240229';

fastify.register(require('@fastify/static'), {
  root: __dirname,
  prefix: '/public/',
});

fastify.register(require('@fastify/multipart'));

let pdfContext = '';
let systemPrompt = '';
let conversationHistory = [];

const callClaudeAPI = async (messages, systemPrompt) => {
  try {
    const response = await axios.post(CLAUDE_API_URL, {
      model: CLAUDE_MODEL,
      messages: messages,
      system: systemPrompt,
      max_tokens: 1000
    }, {
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.CLAUDE_API_KEY,
        'anthropic-version': '2023-06-01'
      }
    });
    return response.data.content[0].text;
  } catch (error) {
    console.error('Error calling Claude API:', error.response ? error.response.data : error.message);
    throw new Error('Failed to get response from Claude API: ' + (error.response ? JSON.stringify(error.response.data) : error.message));
  }
};

fastify.post('/upload', async (request, reply) => {
  const data = await request.file();
  const buffer = await data.toBuffer();
  
  try {
    const pdf = await PDFParser(buffer);
    pdfContext = pdf.text;
    systemPrompt = `You are an assistant that answers questions based on the following PDF content: ${pdfContext}`;
    conversationHistory = []; // Reset conversation history
    return { success: true, message: 'PDF uploaded and processed successfully.' };
  } catch (error) {
    return { success: false, message: 'Error processing PDF: ' + error.message };
  }
});

fastify.post('/ask', async (request, reply) => {
  const { question } = request.body;
  if (!question) {
    return { success: false, message: 'No question provided.' };
  }
  if (!pdfContext) {
    return { success: false, message: 'Please upload a PDF first.' };
  }

  try {
    conversationHistory.push({ role: "user", content: question });
    const answer = await callClaudeAPI(conversationHistory, systemPrompt);
    conversationHistory.push({ role: "assistant", content: answer });
    return { success: true, answer };
  } catch (error) {
    return { success: false, message: error.message };
  }
});

fastify.get('/', async (request, reply) => {
  reply.type('text/html').send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>PDF Chat Interface</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-gray-100">
        <div class="container mx-auto p-4">
            <div class="bg-white shadow-lg rounded-lg p-6">
                <h1 class="text-2xl font-bold mb-4">PDF Chat Interface</h1>
                <div class="mb-4">
                    <input type="file" id="pdfInput" accept=".pdf" class="mb-2">
                    <button onclick="uploadPDF()" class="bg-blue-500 text-white px-4 py-2 rounded">Upload PDF</button>
                </div>
                <div id="chatContainer" class="mt-6 h-96 overflow-y-auto border p-4 mb-4"></div>
                <div class="flex space-x-2">
                    <input type="text" id="questionInput" placeholder="Ask a question" class="flex-grow p-2 border rounded">
                    <button onclick="askQuestion()" class="bg-green-500 text-white px-4 py-2 rounded">Ask</button>
                </div>
            </div>
        </div>
        <script>
            async function uploadPDF() {
                const fileInput = document.getElementById('pdfInput');
                const file = fileInput.files[0];
                if (!file) {
                    alert('Please select a PDF file');
                    return;
                }
                const formData = new FormData();
                formData.append('pdf', file);
                try {
                    const response = await fetch('/upload', {
                        method: 'POST',
                        body: formData
                    });
                    const result = await response.json();
                    alert(result.message);
                    if (result.success) {
                        addMessageToChat('system', 'PDF uploaded successfully. You can now ask questions about its content.');
                    }
                } catch (error) {
                    alert('Error uploading PDF: ' + error.message);
                }
            }

            async function askQuestion() {
                const questionInput = document.getElementById('questionInput');
                const question = questionInput.value.trim();
                if (!question) {
                    alert('Please enter a question');
                    return;
                }
                addMessageToChat('user', question);
                try {
                    const response = await fetch('/ask', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({ question })
                    });
                    const result = await response.json();
                    if (result.success) {
                        addMessageToChat('assistant', result.answer);
                    } else {
                        addMessageToChat('system', 'Error: ' + result.message);
                    }
                    questionInput.value = '';
                } catch (error) {
                    addMessageToChat('system', 'Error asking question: ' + error.message);
                }
            }

            function addMessageToChat(role, content) {
                const chatContainer = document.getElementById('chatContainer');
                const messageDiv = document.createElement('div');
                messageDiv.className = role === 'user' ? 'bg-blue-100 p-2 rounded mb-2' : 'bg-green-100 p-2 rounded mb-2';
                messageDiv.textContent = content;
                chatContainer.appendChild(messageDiv);
                chatContainer.scrollTop = chatContainer.scrollHeight;
            }
        </script>
    </body>
    </html>
  `);
});

const start = async () => {
  try {
    await fastify.listen({ port: PORT });
    console.log(`Server is running on http://localhost:${PORT}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();