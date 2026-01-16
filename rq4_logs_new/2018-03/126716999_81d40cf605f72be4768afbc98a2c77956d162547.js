const express       = require('express'),
      routes        = require('./controllers'),
      app           = express();

app.set('view engine', 'ejs');
app.use(express.static('public'));

// IMPORT ROUTES      
app.use(routes.game);
app.use(routes.question);

app.listen(3000, function(){
    console.log('Server is running...');
});