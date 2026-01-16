const express = require('express');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const sqlite3 = require('sqlite3');
const bodyParser = require('body-parser');
const path = require('path');

const app = express();
const port = 3000;

app.use(cors());

// Middleware pour parser le corps des requêtes
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Chemin vers la base de données SQLite
const dbPath = path.resolve(__dirname, 'donnees.db');
const db = new sqlite3.Database(dbPath);

// Route pour gérer l'inscription
app.post('/inscription', (req, res) => {
    const { nom, prenom, adresse, codePostal, ville, telephone, email, mdp } = req.body;
  
    // Logique de hachage du mot de passe ici
  
    // Utiliser une requête préparée pour insérer les données dans la base de données
    const sql = 'INSERT INTO utilisateurs (nom, prenom, adresse, code_postal, ville, telephone, email, mdp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)';
    const values = [nom, prenom, adresse, codePostal, ville, telephone, email, mdp];
  
    db.run(sql, values, (err) => {
      if (err) {
        console.error('Erreur lors de l\'inscription :', err);
        res.status(500).json({ error: 'Erreur lors de l\'inscription' });
      } else {
        console.log('Utilisateur inscrit avec succès.');
        res.status(200).json({ success: true });
      }
    });
  });


// Route pour la connexion
app.post('/connexion', (req, res) => {
    const { email, mdp } = req.body;
  
    // Utiliser une requête préparée pour vérifier les informations de connexion dans la base de données
    const sql = 'SELECT * FROM utilisateurs WHERE email = ? AND mdp = ?';
    const values = [email, mdp];
  
    db.get(sql, values, (err, row) => {
      if (err) {
        console.error('Erreur lors de la vérification des informations de connexion :', err);
        res.status(500).json({ error: 'Erreur interne du serveur' });
      } else if (row) {
        // Créez un jeton JWT
        const token = jwt.sign({ email }, 'votre_clé_secrète', { expiresIn: '1h' });
        // Utilisateur trouvé, connexion réussie
        res.status(200).json({ success: true, token });
      } else {
        // Aucun utilisateur trouvé avec les informations fournies
        res.status(401).json({ error: 'Informations de connexion incorrectes' });
      }
    });
  });






// Démarrer le serveur
app.listen(port, () => {
  console.log(`Serveur backend en écoute sur le port ${port}`);
});