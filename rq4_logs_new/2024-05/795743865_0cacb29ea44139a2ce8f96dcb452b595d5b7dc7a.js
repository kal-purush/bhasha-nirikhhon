const { pokeneasData, length } = require('../models/pokeneas.js');

function getRandomPokeneaIndex() {
    return Math.floor(Math.random() * length);
}

function getPokeneaData(index) {
    const pokenea = pokeneasData[index];
    return {
        id: pokenea.id,
        nombre: pokenea.nombre,
        altura: pokenea.altura,
        habilidad: pokenea.habilidad,
        imagen: pokenea.imagen,
        fraseFilosofica: pokenea.fraseFilosofica
    };
}

function getRandomPokeneaJSON(req, res) {
    const randomIndex = getRandomPokeneaIndex();
    const data = getPokeneaData(randomIndex);
    res.json(data);
}

function getRandomPokeneaInfo(req, res) {
    const randomIndex = getRandomPokeneaIndex();
    const pokenea = getPokeneaData(randomIndex);
    res.send(`
        <img src="/images/${pokenea.imagen}" alt="${pokenea.nombre}">
        <p>${pokenea.fraseFilosofica}</p>
    `);
}

module.exports = { getRandomPokeneaJSON, getRandomPokeneaInfo };