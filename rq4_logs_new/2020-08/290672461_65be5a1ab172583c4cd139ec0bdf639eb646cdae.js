//Libraries import
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const morgan = require('morgan');
// Express app initialization
const app = express();
// CORS initialization
var corsOptions = {
  origin: 'http://localhost:4200'
};
app.use(cors(corsOptions));
//Morgan initalization
app.use(morgan('combined'));
//Body Parser definitions
// Request with content-type - application/json
app.use(bodyParser.json());
// Request with content-type - application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: true }));

// Test route declcaration
const PORT = process.env.PORT || 3030;
app.get('/', (request, response) => {
  response.json({ 
    message: `legalbot-login-backend-cgv running in port ${PORT}`,
    availableEndpoints: [
      '/api/auth/sign_in',
      '/api/auth/sign_up'
    ]
  });
});

// Routes declaration
require('./src/routes/auth')(app);

// App listen action
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}.`);
});

// DB initialization
const db = require('./src/models');
const db_config = require('./src/config/database');

db.mongoose
  .connect(`mongodb://${db_config.HOST}:${db_config.PORT}/${db_config.DB}`, {
    useNewUrlParser: true,
    useUnifiedTopology: true
  })
  .then(() => {
    console.log('Successfully connect to MongoDB.');
  })
  .catch(err => {
    console.error('Connection error', err);
    process.exit();
  });