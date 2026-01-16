const express = require('express');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 5000;
 Liste des origines autorisées
const allowedOrigins = ['https://todotest121.netlify.app'];

//  Middleware CORS (simplifié)
app.use(cors({
  origin: (origin, callback) => {
    if (!origin || allowedOrigins.includes(origin)) {
      return callback(null, true);
    }
    return callback(new Error('Origine non autorisée par CORS'));
  }
}));

//  Middleware pour parser le JSON
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

//  Route test
app.get('/', (req, res) => {
  res.json({ message: 'API opérationnelle' });
});

//  Données simulées
let tasks = [
  { id: 1, title: 'Apprendre Express', completed: false },
  { id: 2, title: 'Créer une API REST', completed: false }
];

//  GET : toutes les tâches
app.get('/api/tasks', (req, res) => {
  res.json(tasks);
});
 GET : une tâche spécifique
app.get('/api/tasks/:id', (req, res) => {
  const task = tasks.find(t => t.id === +req.params.id);
  if (!task) return res.status(404).json({ error: 'Tâche non trouvée' });
  res.json(task);
});

//POST : ajouter une tâche
app.post('/api/tasks', (req, res) => {
  const { title } = req.body;
  if (!title) return res.status(400).json({ error: 'Le titre est requis' });

  const newTask = {
    id: tasks.length + 1,
    title,
    completed: false
  };

  tasks.push(newTask);
  res.status(201).json(newTask);
});

//  PUT : modifier une tâche
app.put('/api/tasks/:id', (req, res) => {
  const task = tasks.find(t => t.id === +req.params.id);
  if (!task) return res.status(404).json({ error: 'Tâche non trouvée' });

  const { title, completed } = req.body;
  if (title !== undefined) task.title = title;
  if (completed !== undefined) task.completed = completed;

  res.json(task);
});

// DELETE : supprimer une tâche
app.delete('/api/tasks/:id', (req, res) => {
  const id = +req.params.id;
  const index = tasks.findIndex(t => t.id === id);
  if (index === -1) return res.status(404).json({ error: 'Tâche non trouvée' });

  tasks.splice(index, 1);
  res.json({ message: 'Tâche supprimée' });
});

//  Démarrage du serveur
app.listen(PORT, () => {
  console.log(`✅ Serveur lancé sur http://localhost:${PORT}`);
});