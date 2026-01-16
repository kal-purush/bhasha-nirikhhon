const mongoose = require('mongoose');

// Replace the <username> and <password> placeholders with your actual MongoDB username and password
const username = '<username>';
const password = '<password>';
const MONGO_URI = process.env.MONGO_DB || "mongodb://127.0.0.1:27017/OLINA";



mongoose.connect(`mongodb+srv://${username}:${password}@cluster0.mongodb.net/OLINA?retryWrites=true&w=majority`, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
}).then(() => {
  console.log('Connected to MongoDB');
}).catch((err) => {
  console.error(err);
});