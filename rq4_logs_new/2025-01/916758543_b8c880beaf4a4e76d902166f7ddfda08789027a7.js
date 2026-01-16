//Esempio: Sistema di Autenticazione con Token JWT (json web token)

const express = require('express');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs'); 

const app = express();
const PORT = process.env.PORT || 3000;
const SECRET_KEY = 'my_secret_key';

app.use(express.json());

let users = [];

//register
app.post('/register', (req, res) => {
    const { username, password } = req.body;

    const hashedPassword = bcrypt.hashSync(password, 10);
    users.push({ username, password: hashedPassword});

    res.status(201).send('User registered successfully');
});

// Login
app.post('/login', async (req, res) => {
    const { username, password } = req.body;
    const user = users.find(u => u.username === username);  
    
    if (!user || !(await bcrypt.compare(password, user.password))) {  
      return res.status(401).send('Invalid credentials');
    }
  
    // Genera token JWT
    const token = jwt.sign({ username }, SECRET_KEY, { expiresIn: '1h' });
    res.json({ token });
  });
  

//middleware protect private routs
const authMiddleware = (req, res, next) => {
    const token = req.headers.authorization?.split(' ')[1];
    if (!token) return res.status(401).send('Unauthorized');

    try {
        const decoded = jwt.verify(token, SECRET_KEY);
        req.user = decoded;
        next();
    } catch (err) {
        res.status(401).send('token not valid or expired'); 
    }
};

//private protected route
app.get('/profile', authMiddleware, (req, res) => {
    res.send(`Welcome on your profile, ${req.user.username}`);
});

//start server
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});