// api/processTask.js - Process a single task
const axios = require('axios');

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization'
};

module.exports = async (req, res) => {
  // Set CORS headers
  Object.entries(corsHeaders).forEach(([key, value]) => {
    res.setHeader(key, value);
  });

  // Handle preflight
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  // Only allow POST
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { taskId, status } = req.body;
  const apiKey = req.headers.authorization;
  
  if (!apiKey || !taskId || !status) {
    return res.status(400).json({ 
      error: 'Missing required parameters', 
      message: 'API key, taskId, and status are required' 
    });
  }
  
  // Clean API key
  const cleanApiKey = apiKey.replace(/^Bearer\s+/i, '');
  
  try {
    const response = await axios({
      method: 'PUT',
      url: `https://api.clickup.com/api/v2/task/${taskId}`,
      headers: {
        'Authorization': cleanApiKey,
        'Content-Type': 'application/json'
      },
      data: { status },
      timeout: 5000
    });
    
    return res.status(200).json({ 
      taskId, 
      success: true, 
      message: 'Status updated successfully' 
    });
  } catch (error) {
    console.error(`Error updating task ${taskId}:`, error.message);
    
    return res.status(200).json({ 
      taskId, 
      success: false, 
      message: `Failed: ${error.response?.data?.err || error.message}` 
    });
  }
};