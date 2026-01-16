const API_KEY = "sk-JMxfSfmxgpX1CPuXRvl6eM5d93WglbkZpw44CKLYHcWbmO26";  // 替换为你的 Kimi API Key
const API_URL = "https://api.moonshot.cn/v1/chat/completions";  // 替换为 Kimi API 端点

async function sendMessage() {
    const userInput = document.getElementById('userInput');
    const message = userInput.value.trim();
    
    if (!message) return;

    // 显示用户消息
    addMessage(message, 'user');
    userInput.value = '';

    try {
        // 发送请求到 Kimi API
        const response = await fetch(API_URL, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${API_KEY}`
            },
            body: JSON.stringify({
                model: "moonshot-v1-8k",  // 模型名称，根据实际 API 配置
                messages: [
                    { role: "user", content: message }
                ]
            })
        });

        // 检查是否请求成功
        if (!response.ok) {
            const errorDetails = await response.text();
            console.error("API 请求失败，响应状态:", response.status);
            console.error("错误详情：", errorDetails);
            throw new Error("API 请求失败");
        }

        const data = await response.json();
        
        if (!data || !data.choices || !data.choices[0]?.message?.content) {
            console.error("API 返回的数据格式错误:", data);
            throw new Error("AI 没有返回有效的消息");
        }

        const reply = data.choices[0].message.content;
        // 显示 AI 消息
        addMessage(reply, 'ai');
    } catch (error) {
        console.error("请求错误", error);
        addMessage("AI 服务器连接失败，请稍后重试。错误详情：" + error.message, 'ai');
    }
}

function addMessage(content, sender) {
    const container = document.getElementById('chatContainer');
    
    const messageDiv = document.createElement('div');
    messageDiv.className = `message ${sender}-message`;
    
    const avatarDiv = document.createElement('div');
    avatarDiv.className = `avatar ${sender}-avatar`;
    
    const contentDiv = document.createElement('div');
    contentDiv.className = 'content';
    contentDiv.textContent = content;
    
    messageDiv.appendChild(avatarDiv);
    messageDiv.appendChild(contentDiv);
    container.appendChild(messageDiv);
    
    // 自动滚动到底部
    container.scrollTop = container.scrollHeight;
}

// 监听回车键发送消息
document.getElementById('userInput').addEventListener('keypress', function(e) {
    if (e.key === 'Enter') {
        sendMessage();
    }
});

// 侧拉栏功能
function toggleSidebar() {
    const sidebar = document.getElementById('sidebar');
    sidebar.style.display = sidebar.style.display === 'block' ? 'none' : 'block';
}

function loadChat(chatId) {
    console.log(`加载会话 ${chatId}`);
    // 在这里可以通过 chatId 加载历史消息
}

function startNewChat() {
    console.log("开始新会话");
    // 这里可以重置当前聊天界面
    document.getElementById('chatContainer').innerHTML = ''; // 清空聊天
    addMessage("您好！我是AI助手，有什么可以帮您的？", 'ai'); // 显示欢迎消息
}
