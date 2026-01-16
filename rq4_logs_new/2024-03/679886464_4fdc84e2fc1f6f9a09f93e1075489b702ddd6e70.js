const express = require("express")
const rota = express();

const Mensagem = require("../modelos/Mensagem")


rota.get("/mensagensImportantes",async (req,res)=>{
    const importantes = await Mensagem.importantes();

    res.json(importantes)

})

module.exports = rota