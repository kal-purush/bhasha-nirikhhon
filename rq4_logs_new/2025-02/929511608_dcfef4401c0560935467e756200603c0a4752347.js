document.addEventListener('DOMContentLoaded', function () {
    const chatbotToggle = document.createElement('button');
    chatbotToggle.textContent = 'Chat with Us';
    chatbotToggle.style.position = 'fixed';
    chatbotToggle.style.bottom = '20px';
    chatbotToggle.style.right = '20px';
    chatbotToggle.style.padding = '10px';
    chatbotToggle.style.backgroundColor = '#0073e6';
    chatbotToggle.style.color = 'white';
    chatbotToggle.style.border = 'none';
    chatbotToggle.style.cursor = 'pointer';

    document.body.appendChild(chatbotToggle);

    const chatbotContainer = document.createElement('div');
    chatbotContainer.style.display = 'none';
    chatbotContainer.style.position = 'fixed';
    chatbotContainer.style.bottom = '70px';
    chatbotContainer.style.right = '20px';
    chatbotContainer.style.width = '300px';
    chatbotContainer.style.height = '400px';
    chatbotContainer.style.background = 'white';
    chatbotContainer.style.border = '1px solid #ccc';
    chatbotContainer.style.boxShadow = '0 0 10px rgba(0, 0, 0, 0.1)';
    chatbotContainer.style.overflow = 'hidden';
    chatbotContainer.style.padding = '10px';

    const chatbotMessages = document.createElement('div');
    chatbotMessages.style.height = '80%';
    chatbotMessages.style.overflowY = 'auto';
    chatbotMessages.style.padding = '10px';
    chatbotMessages.style.borderBottom = '1px solid #ddd';

    const chatbotInput = document.createElement('input');
    chatbotInput.type = 'text';
    chatbotInput.placeholder = 'Ask a question...';
    chatbotInput.style.width = 'calc(100% - 20px)';
    chatbotInput.style.padding = '5px';
    chatbotInput.style.margin = '5px';

    const chatbotSend = document.createElement('button');
    chatbotSend.textContent = 'Send';
    chatbotSend.style.width = '100%';
    chatbotSend.style.padding = '10px';
    chatbotSend.style.backgroundColor = '#0073e6';
    chatbotSend.style.color = 'white';
    chatbotSend.style.border = 'none';
    chatbotSend.style.cursor = 'pointer';

    chatbotContainer.appendChild(chatbotMessages);
    chatbotContainer.appendChild(chatbotInput);
    chatbotContainer.appendChild(chatbotSend);

    document.body.appendChild(chatbotContainer);

    chatbotToggle.addEventListener('click', function () {
        chatbotContainer.style.display = chatbotContainer.style.display === 'none' || chatbotContainer.style.display === '' ? 'block' : 'none';
    });

    chatbotSend.addEventListener('click', async function () {
        if (chatbotInput.value.trim() !== '') {
            const userMessage = document.createElement('p');
            userMessage.textContent = 'You: ' + chatbotInput.value;
            chatbotMessages.appendChild(userMessage);

            const botMessage = document.createElement('p');
            botMessage.textContent = 'Please wait...';
            chatbotMessages.appendChild(botMessage);

            const response = await getBotResponse(chatbotInput.value);
            botMessage.textContent = 'Bot: ' + response;

            chatbotInput.value = '';
            chatbotMessages.scrollTop = chatbotMessages.scrollHeight;
        }
    });

    async function getBotResponse(input) {
        const apiKey = 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; 
        const response = await fetch('https://api.openai.com/v1/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model: 'text-davinci-003',
                prompt: input,
                max_tokens: 100
            })
        });

        const data = await response.json();
        return data.choices[0].text.trim();
        
    }
});