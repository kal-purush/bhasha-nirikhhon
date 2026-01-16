// config/straddle.js
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const https = require('https');
require('dotenv').config();
// Create an instance of axios for Straddle API


const straddleClient = axios.create({
 baseURL: process.env.STRADDLE_API_BASE_URL,
  httpsAgent: new https.Agent({
    rejectUnauthorized: false
  }),
  headers: {
    'Authorization': `Bearer ${process.env.STRADDLE_API_KEY}`,
    'Content-Type': 'application/json',
    'Straddle-Account-Id': process.env.STRADDLE_ACCOUNT_ID,
     'request-id': uuidv4(),           // Optional
  'correlation-id': uuidv4()   
  }
});


async function getCustomerByEmail(email) {
  
  try {
    const response = await straddleClient.get('/customers', { params: { email } });
   if (response.data.data && response.data.data?.length > 0) {
      return response.data.data[0]; // assume first match
    }
    return null;
  } catch (e) {
    console.error("Failed to get customer by email:", e);
    return null;
  }
}

// 1. Create Customer
async function createStraddleCustomer({ name, email, phone, ip_address }) {
  
    
  try {
    const response = await straddleClient.post('/customers', {
      name,
      type: 'individual',
      email,
      phone,
      device: {
        ip_address
      }
    });

    return response.data;
  } catch (error) {
    const errorDetail = error.response?.data?.error?.detail || "";
  if (errorDetail.includes("already exists")) {

  const existingCustomer = await getCustomerByEmail(email);

  if (existingCustomer?.id) {
    console.warn("Customer already exists, fetched existing ID:", existingCustomer.id);
    return { id: existingCustomer.id, alreadyExists: true };
  }
  throw new Error(`Straddle Customer Creation Error: ${errorDetail}`);
}


  }
}

// 2. Link Plaid Token to Customer
async function linkPlaidTokenToStraddle({ customer_id, plaid_token }) {
  try {
    const response = await straddleClient.post('/bridge/plaid', {
      customer_id,
      plaid_token
    });

    return response.data;
  } catch (error) {
   const errorDetail = error.response?.data?.error?.detail;
   const items = error?.response?.data?.error?.items || [];console.log("items",items);
 
const itemMessages = items.map(i => i.detail).join('\n');
console.log("itemMessages",itemMessages);
console.error('Straddle Bridge Error:', errorDetail);
throw new Error(`Straddle Bridge Error: ${errorDetail}`);
  }
}

// 3. Create Payout
async function createStraddlePayout({ paykey, amount, description, payment_date, external_id, ip_address }) {
  try {
    const response = await straddleClient.post('/payouts', {
      paykey,
      amount,
      currency: 'USD',
      description,
      payment_date,
      external_id,
      device: {
        ip_address
      }
    });

    return response.data;
  } catch (error) {
  
const items = error?.response?.data?.error?.items || [];console.log("items",items);

const itemMessages = items.map(i => i.detail).join('\n');
console.log("itemMessages",itemMessages);
const fullMessage = `Validation failed:\n${itemMessages}`;

console.error('Straddle Payout Error:\n', fullMessage);
throw new Error(`Straddle Payout Error: ${fullMessage}`);

  }
}



module.exports = {straddleClient,createStraddleCustomer,
  linkPlaidTokenToStraddle,
  createStraddlePayout};