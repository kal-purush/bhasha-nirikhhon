// Importar módulos con require
const express = require("express");
const mysql = require("mysql");
const cors = require("cors");
const jwt = require("jsonwebtoken"); // Importar jsonwebtoken

// Configuración del servidor
const app = express();
app.use(express.json());
app.use(cors());

const SECRET_KEY = "6$ty9EyLqWHPJzVd$KFtV7MK3"; // Cambia esto a una clave secreta más segura

// Conexión a base de datos
const Conexión = mysql.createConnection({
    host: "localhost",
    port: 3306,
    database: "datos usuario", // Nombre correcto de la base de datos
    user: "root",
    password: ""
});

// Abrir la conexión a la base de datos
Conexión.connect((err) => {
    if (err) {
        console.error("Error al conectar a la base de datos:", err);
        return;
    }
    console.log("Conexión a la base de datos establecida con éxito.");
});

// Ruta para iniciar sesión
app.post('/login', (req, res) => {
    const { email, password } = req.body; // Desestructurar email, password y apiKey del cuerpo de la solicitud
    const apiKey = req.headers['authorization'] && req.headers['authorization'].split(' ')[1]; // Extraer la apiKey del encabezado


    console.log("apiKey",apiKey)
    // Validar apiKey
    if (apiKey !== SECRET_KEY) { // Reemplaza 'YOUR_API_KEY' con la clave real
        return res.status(403).json("API key incorrecta."); // Respuesta si la apiKey es incorrecta
    }

    const db = "SELECT * FROM users_test WHERE email = ? AND contraseña = ?";
    Conexión.query(db, [email, password], (err, data) => {
        if (err) return res.status(500).json("Error en inicio de sesión");

        if (data.length > 0) {
            const token = jwt.sign({ email: email }, SECRET_KEY, { expiresIn: '1h' }); // Generar un token JWT
            return res.status(200).json({ token }); // Enviar el token al frontend
        } else {
            return res.status(401).json("Usuario incorrecto"); // Respuesta si las credenciales son incorrectas
        }
    });
});

// Ruta para registrar un nuevo usuario
app.post('/register', (req, res) => {
    const { fullName, age, gender, address, phone, email, password, confirmPassword } = req.body;

    // Log para verificar los campos recibidos
    console.log("Datos recibidos para registro:", req.body);

    // Validar que todos los campos estén presentes
    if (!fullName || !age || !gender || !address || !phone || !email || !password || !confirmPassword) {
        return res.status(400).json("Todos los campos son requeridos.");
    }

    // Consulta para insertar un nuevo usuario
    const query = `
        INSERT INTO users_test (nombreCompleto, edad, genero, direccion, Celular, email, contraseña, confirmarContraseña)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `;

    Conexión.query(query, [fullName, age, gender, address, phone, email, password, confirmPassword], (err, result) => {
        if (err) {
            console.error("Error al registrar usuario:", err);
            return res.status(500).json("Error al registrar usuario.");
        }

        return res.status(200).json("Registro exitoso.");
    });
});

// Middleware para autenticar el token
function authenticateToken(req, res, next) {
    const token = req.headers['authorization'] && req.headers['authorization'].split(' ')[1];

    if (!token) return res.sendStatus(401); // Si no hay token, retorna 401

    jwt.verify(token, SECRET_KEY, (err, user) => {
        if (err) return res.sendStatus(403); // Si el token no es válido, retorna 403
        req.user = user; // Si es válido, guarda el usuario en la solicitud
        next(); // Llama al siguiente middleware
    });
}

// Ruta protegida para obtener datos del usuario
app.get('/protected', authenticateToken, (req, res) => {
    res.status(200).json({ message: "Esta es una ruta protegida", user: req.user });
});

// Poner a escuchar el servidor
app.listen(3000, () => {
    console.log("Servidor escuchando en el puerto 3000...");
});