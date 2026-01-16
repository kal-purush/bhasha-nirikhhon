const express = require('express');
const dotenv = require('dotenv');
const cors = require('cors');
const conectarBDMongo = require('./src/configuracion/baseDatos'); // MongoDB
const { conectarBDPostgres } = require('./src/configuracion/baseDatosPostgres'); // PostgreSQL
const middlewareAutenticacion = require('./src/middleware/middlewareAutenticacion');

dotenv.config();

const app = express();


app.use(express.json());
app.use(cors({
    origin:'http://localhost:3000',
    methods:["GET", "POST", "PUT", "DELETE"],
    credentials:true,
}));

conectarBDMongo(); 
conectarBDPostgres(); 


app.use('/api/productos', require('./src/rutas/rutasProducto'));
app.use('/api/usuarios', require('./src/rutas/rutasUsuario'));
app.use('/api/ordenes', require('./src/rutas/rutasOrden'));
app.use('/api/carrito', require('./src/rutas/rutasCarrito'));
app.use('/api/categorias', require('./src/rutas/rutasCategoria'));


app.get('/api/usuarios/perfil', middlewareAutenticacion, (req, res) => {
    res.status(200).json({ mensaje: 'Perfil de usuario', usuario: req.user });
});


app.use((req, res, next) => {
    res.status(404).json({ mensaje: 'Ruta no encontrada' });
});


app.use((error, req, res, next) => {
    console.error(error.stack);
    res.status(500).json({ mensaje: 'Error en el servidor', error: error.message });
});


const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log(`Servidor ejecutándose en el puerto ${PORT}`);
});