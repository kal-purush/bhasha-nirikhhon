const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');

const app = express();
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static('public')); // Serving static files from the 'public' directory

const azureEndpoint = 'YOUR_AZURE_API_ENDPOINT';
const subscriptionKey = 'YOUR_AZURE_SUBSCRIPTION_KEY';

app.get('/', (req, res) => {
    res.sendFile(__dirname + '/public/index.html');
});

app.post('/analyze', async (req, res) => {
    const text = req.body.text;

    try {
        const response = await axios.post(`${azureEndpoint}/sentiment`, {
            documents: [
                {
                    language: 'en',
                    id: '1',
                    text: text,
                },
            ],
        }, {
            headers: {
                'Content-Type': 'application/json',
                'Ocp-Apim-Subscription-Key': subscriptionKey,
            },
        });

        const sentimentScore = response.data.documents[0].score;
        const sentimentLabel = sentimentScore > 0.5 ? 'Positive' : sentimentScore < 0.5 ? 'Negative' : 'Neutral';

        res.json({ sentimentLabel, sentimentScore });
    } catch (error) {
        console.error('Error analyzing sentiment:', error.message);
        res.status(500).json({ error: 'An error occurred while analyzing sentiment.' });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});