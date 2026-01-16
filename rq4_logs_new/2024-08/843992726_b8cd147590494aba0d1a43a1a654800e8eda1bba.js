// const express = require('express');
// const bodyParser = require('body-parser');
// const cors = require('cors');
// const mongoose = require('mongoose');
// const multer = require('multer');
// const Identity = require('./models/model');

// const app = express();
// app.use(cors());
// app.use(bodyParser.json());

// mongoose.connect('mongodb://127.0.0.1:27017/Angularidentity', { useNewUrlParser: true, useUnifiedTopology: true });



// app.post("/users/register", (req, res) => {
//   const { name, father, mother, gender, mobile, email, address } = req.body;
  
// const newIdentity = new Identity({ name, father, mother, gender, mobile, email, address });

  
//   // Save the new document to the database
//   newIdentity.save()
//     .then(savedIdentity => res.json(savedIdentity))
//     .catch(err => res.status(500).json({ error: err.message }));
// });

// app.get("/users/:id", async (req,res)=>{
//   try{
//     const _id = req.params.id;
//     const getIdentity = await Identity
//     .findById(_id);
//     res.send(getIdentity);
//   }catch(e){
//     res.status(400).send(e);
//   }
// })

// app.get("/users", async (req, res) => {
//   try {
//     const users = await Identity.find();
//     res.send(users);
//   } catch (e) {
//     res.status(400).send(e);
//   }
// });


// app.patch("/users/:id", async (req,res)=>{
//   try{
//     const _id = req.params.id;
//     const updateIdentity = await Identity
//     .findByIdAndUpdate(_id,req.body,{new:true
//     });
//     res.send(updateIdentity);
//   }catch(e){
//     res.status(400).send(e);
//   }
// })

// app.delete("/users/:id", async (req,res)=>{
//   try{
//     const _id = req.params.id;
//     const deleteIdentity = await Identity
//     .findByIdAndDelete(_id,req.body,);
//     res.send(deleteIdentity);
//   }catch(e){
//     res.status(400).send(e);
//   }
// })
// const PORT = 3000;
// app.listen(PORT, () => {
//   console.log(`Server is running on http://localhost:${PORT}`);
// });
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const mongoose = require('mongoose');
const Identity = require('./models/model');

const app = express();
app.use(cors());
app.use(bodyParser.json());

mongoose.connect('mongodb://127.0.0.1:27017/Angularidentity', { useNewUrlParser: true, useUnifiedTopology: true });

app.post("/users/register", (req, res) => {
  const { name, father, mother, gender, mobile, email, address } = req.body;
  const newIdentity = new Identity({ name, father, mother, gender, mobile, email, address });

  newIdentity.save()
    .then(savedIdentity => res.json(savedIdentity))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.get("/users/:id", async (req, res) => {
  try {
    const _id = req.params.id;
    const getIdentity = await Identity.findById(_id);
    res.send(getIdentity);
  } catch (e) {
    res.status(400).send(e);
  }
});

app.patch("/users/:id", async (req, res) => {
  try {
    const _id = req.params.id;
    const updateIdentity = await Identity.findByIdAndUpdate(_id, req.body, { new: true });
    res.send(updateIdentity);
  } catch (e) {
    res.status(400).send(e);
  }
});

app.get("/users", async (req, res) => {
  try {
    const getIdentities = await Identity.find();
    res.send(getIdentities);
  } catch (e) {
    res.status(400).send(e);
  }
});

app.delete("/users/:id", async (req, res) => {
  try {
    const _id = req.params.id;
    const deleteIdentity = await Identity.findByIdAndDelete(_id);
    res.send(deleteIdentity);
  } catch (e) {
    res.status(400).send(e);
  }
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});