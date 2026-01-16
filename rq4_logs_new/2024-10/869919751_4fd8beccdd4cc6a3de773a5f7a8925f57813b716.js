const express = require("express");
const axios = require("axios");
const app = express();
const port = 3000;

// Fonction pour obtenir les routes Traefik
async function fetchTraefikRoutes() {
  try {
    const response = await axios.get("http://traefik:8080/api/http/routers");
    const routers = response.data;
    let ret = [];

    for (const router in routers) {
      if (routers[router].rule) {
        const hostRule = routers[router].rule.match(/Host\(`([^`]+)`\)/);
        if (hostRule && hostRule[1]) {
          ret.push(hostRule[1]);
        }
      }
    }
    return ret;
        
  } catch (error) {
    console.error("Erreur lors de la récupération des routes de Traefik:", error);
    return [];
  }
}

// Route principale pour afficher les liens
app.get("/", async (req, res) => {
  const routes = await fetchTraefikRoutes();
  console.log(routes);
  
  const htmlContent = `
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Liste des domaines</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                background-color: #f4f4f9;
                color: #333;
                text-align: center;
                padding: 20px;
            }
            h1 {
                color: #4CAF50;
                font-size: 2.5em;
            }
            a {
                display:block;    
                background-color: #fff;
                margin: 10px;
                padding: 15px;
                border-radius: 5px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                transition: transform 0.2s ease;
                text-decoration: none;
                color: #2196F3;
                font-weight: bold;
            }
            a:hover {
                color: #0b7dda;
            }
        </style>
    </head>
    <body>
        <h1>Domains Configured in Traefik</h1>
        ${routes.map(route => `<div><a href="https://${route}">${route}</a></div>`).join('')}
    </body>
    </html>
  `;

  res.send(htmlContent);
});

// Démarrage du serveur
app.listen(port, () => {
  console.log(`Serveur en écoute sur http://localhost:${port}`);
});