const express = require("express");
const cors = require("cors");
const app = express();

const PORT = process.env.PORT || 8080;
const db = require("./config/model.config");
//require('./config/routes.config')(app);
const Role = db.role;
app.use(cors({origin: '*'}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

  require('./auth/routes/auth.routes')(app);
  require('./users/routes/user.routes')(app);
  require('./profile/routes/profile.routes')(app);
  require('./category/routes/category.routes')(app);
  require('./questions/routes/question.routes')(app);
  require('./course/routes/course.routes')(app);
  require('./quiz/routes/quiz.routes')(app);
  
//db.sequelize.sync();

//Para generar todo de 0 descomentar esta linea(24) y comentar la 21
db.sequelize.sync({force: true}).then(() => {initial();});

//server on
app.listen(PORT, () => {
  console.log(`Servidor on Puerto: ${PORT}.`);
});

function initial() {
  Role.create({
    id: 1,
    name: "user"
  });
 
  Role.create({
    id: 2,
    name: "moderator"
  });
 
  Role.create({
    id: 3,
    name: "admin"
  });
}