import express from "express";
import fs from "fs";
import path from "path"; // for reading file
import cors from 'cors';

const app = express();
const port = 3001;

interface Question {
  id: number;
  title: string;
  description: string;
  category: string;
  complexity: string;
}

interface User {
  username: string;
  password: string;
}

// Enable CORS for all routes (Not recommended for production)
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});

// enable DELETE HTTP methods
const corsOptions = {
  origin: 'http://localhost:5173',
  methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
};

app.use(cors(corsOptions));

app.use('/images', express.static(path.join(__dirname, 'data/images')));

app.use(express.json())

app.get('/questions', (req, res) => {
  fs.readFile(path.join(__dirname, 'data/sampleQuestions.json'), 'utf8', (err, data) => {
    if (err) {
      return res.status(500).send('An error occurred');
    }
    return res.json(JSON.parse(data));
  });
});

// allow addition into question bank
app.post('/addQuestion', (req, res) => {
  const newQuestion: Partial<Question> = req.body;
  const questions: Question[] = JSON.parse(fs.readFileSync('data/sampleQuestions.json', 'utf8'));

  // Generate a new ID for the question
  newQuestion.id = questions.length ? Math.max(...questions.map((q: Question) => q.id)) + 1 : 1;

  // Add the new question
  questions.push(newQuestion as Question);

  // Save back to the JSON file
  fs.writeFileSync('data/sampleQuestions.json', JSON.stringify(questions, null, 2));
  res.status(201).send('New question added');
});

// Check password and login
app.post('/login', (req, res) => {
  const { username, password } = req.body;

  const users = JSON.parse(fs.readFileSync('data/users.json', 'utf-8'));

  // Search for a match
  const foundUser = users.find((user: { username: string, password: string }) => user.username === username && user.password === password);

  if (foundUser) {
    return res.status(200).json({ message: 'Login successful' });
  }

  return res.status(401).json({ message: 'Invalid username or password' });
});

// allow update of password
app.put('/login/:username', (req, res) => {
  const username = req.params.username;
  const oldPassword = req.body.oldPassword;
  const newPassword = req.body.newPassword;
  const users: User[] = JSON.parse(fs.readFileSync('data/users.json', 'utf8'));

  const user = users.find((u: User) => u.username === username);

  if (!user) {
      res.status(401).send('User not found');
      return;
  }

  if (user.password !== oldPassword) {
    res.status(401).send('Old password is incorrect');
    return;
  }

  user.password = newPassword;

  fs.writeFileSync('data/users.json', JSON.stringify(users, null, 2));
  res.status(200).send('Password changed successfully');
});


// Delete a question by ID
app.delete('/questions/:id', (req, res) => {
  const id = parseInt(req.params.id, 10);
  let questions: Question[] = JSON.parse(fs.readFileSync('data/sampleQuestions.json', 'utf8'));

  questions = questions.filter((q) => q.id !== id);
  fs.writeFileSync('data/sampleQuestions.json', JSON.stringify(questions, null, 2));
  res.status(200).send('Question deleted');
});

// Update question
app.put('/questions/:id', (req, res) => {
  const id = parseInt(req.params.id, 10);
  const updatedQuestion = req.body;
  const questions = JSON.parse(fs.readFileSync('data/sampleQuestions.json', 'utf8'));

  // Find the index of the question that has the same id as `updatedQuestion.id`
  const index = questions.findIndex((q: Question) => q.id === id);
  if (index !== -1) {
    // Update the question at the found index
    questions[index] = updatedQuestion;
    fs.writeFileSync('data/sampleQuestions.json', JSON.stringify(questions, null, 2));
    res.status(200).send('Question updated');
  } else {
    res.status(404).send('Question not found');
  }
});  


app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});



