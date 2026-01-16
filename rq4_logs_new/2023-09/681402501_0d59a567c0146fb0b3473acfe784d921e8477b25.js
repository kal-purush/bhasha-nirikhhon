import swaggerUI from "swagger-ui-express";
import swaggerJSDoc from "swagger-jsdoc";

// Metada

const options = {
    definition: {
        openapi: "3.0.1",
        info:{ title: 'VET API', version: '1.0.0' },
    },
    apis: ['src/routers/paciente_routes.js' , 'src/routers/veterinario_routes.js' , 'src/database.js' ],
}

// Documentacion en JSON format

const  swaggerSpec = swaggerJSDoc(options);

// Function to setup our docs

const swaggerDocs = (app, port) => {
    app.use('/api/v1/docs', swaggerUI.serve, swaggerUI.setup(swaggerSpec));
    app.get('/api/v1.docs.json', (req, res) =>{
        res.setHeader('Content-type', 'application/json');
        res.send(swaggerSpec)
    });

    console.log(
        ` 📃 Version 1 Docs are available at http://localhost:${app.get('port')}/api/v1/docs`)
};

export {swaggerDocs}
