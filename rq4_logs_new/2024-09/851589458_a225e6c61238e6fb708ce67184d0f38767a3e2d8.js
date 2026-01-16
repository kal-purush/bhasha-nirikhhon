const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const app = express();
app.use(express.json())

const port = 3000;
mongoose.connect('mongodb://localhost:27017/ticketdb', {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });

  mongoose.connection.on('connected', () => {
    console.log('Connected to MongoDB successfully!');
  });
  
  mongoose.connection.on('error', (err) => {
    console.error('Error connecting to MongoDB:', err);
  });

app.use(cors({
    origin: 'http://localhost:4200',
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
  }));

  const ticketSchema = new mongoose.Schema({
    title: String,
    description: String,
    status: String,
  });

  const Ticket = mongoose.model('Ticket', ticketSchema);



// let issues = [
//     { id: 1, title: 'First Issue', description: 'Description of first issue' },
//     { id: 2, title: 'Second Issue', description: 'Description of second issue' },
//     { id: 3, title: 'Third Issue', description: 'Description of third issue' },
// ]


// READ ALL/ GET request
app.get('/issues', async(req, res) => {
    try {
        const tickets = await Ticket.find();
        res.json(tickets);
      } catch (error) {
        res.status(500).json({ message: 'Error fetching tickets' });
      }
})


// CREATE/POST request
app.post('/issues', async(req, res) => {
    try {
        const newTicket = new Ticket(req.body);
        await newTicket.save();
        res.status(201).json(newTicket);
      } catch (error) {
        res.status(500).json({ message: 'Error creating ticket' });
      }
})

// UPDATE/PUT request

app.put('/issues/:id', async(req, res) => {
    try {
        const updatedTicket = await Ticket.findByIdAndUpdate(req.params.id, req.body, { new: true });
        console.log("Updated Ticket", updatedTicket, req.params.id)
        res.json(updatedTicket);
      } catch (error) {
        res.status(500).json({ message: 'Error updating ticket' });
      }
})

//DELETE/ delete operation
app.delete('/issues/:id', async(req, res) => {
    try {
        await Ticket.findByIdAndDelete(req.params.id);
        res.json({ message: 'Ticket deleted' });
      } catch (error) {
        res.status(500).json({ message: 'Error deleting ticket' });
      }
})

app.listen(port, () => {
    console.log(`Server is running at http://localhost:${port}`) 
})