import axios from 'axios';

import dotenv from 'dotenv';
import { MongoClient } from 'mongodb';

  dotenv.config();

const openai_key= process.env.OPENAI_API_KEY

const mongodb_URL= process.env.MONGO_DB_URL

async function getEmbedding(query) {
    // Define the OpenAI API url and key.
    const url = 'https://api.openai.com/v1/embeddings';
    
    // Call OpenAI API to get the embeddings.
    let response = await axios.post(url, {
        input: query,
        model: "text-embedding-ada-002"
    }, {
        headers: {
            'Authorization': `Bearer ${openai_key}`,
            'Content-Type': 'application/json'
        }
    });
    
    if(response.status === 200) {
        return response.data.data[0].embedding;
    } else {
        throw new Error(`Failed to get embedding. Status code: ${response.status}`);
    }
}

async function findSimilarDocuments(embedding) {
    const url = mongodb_URL; // Replace with your MongoDB url.
    const client = new MongoClient(url);
    
    try {
        await client.connect();
        
        const db = client.db('stock'); // Replace with your database name.
        const collection = db.collection('stock-min-data'); // Replace with your collection name.
        
        const sample = await collection.findOne({}, { projection: { vector: 1 } });
            console.log("Stored vector length:", sample.vector.length); 
            console.log("Query vector length:", embedding.length);


        // Query for similar documents.
        const documents = await collection.aggregate([
  {"$vectorSearch": {
    "queryVector": embedding,
    "path": "vector",
    "numCandidates": 50,
    "limit": 5,
    "index": "vector",
      }}
]).toArray();
        
        return documents;
    } finally {
        await client.close();
    }
}
import readline from 'readline';

async function main() {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

     rl.question('Please enter your query: ', async (query) => {
        try {
            console.log('query');
            const embedding = await getEmbedding(query);
            const documents = await findSimilarDocuments(embedding);
            
            console.log(documents);
        } catch(err) {
            console.error(err);
        } finally {
            rl.close();
        }
    });
}



main();