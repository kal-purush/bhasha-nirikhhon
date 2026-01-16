const express = require('express');
const app = express();

app.get('/', (req, resp) => {
    resp.send("Welcome to the server!");
});

const port = process.env.PORT || 5000;
app.listen(port, console.log(`Server is running on port ${port}`));