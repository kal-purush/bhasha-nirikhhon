const express = require('express');
const router = express.Router();

const modelCategoria = require('../model/CategoriaModel');

router.post('/inserirCategoria', (req, res)=>{
    let {NOME_CATEGORIA} = req.body;

    modelCategoria.create(
        {NOME_CATEGORIA}
    ).then(
        ()=>{
                return res.status(201).json({
                    erroStatus: false,
                    menssagemStatus: 'Categoria inserida com sucesso!'
            });
        }
    ).catch(
        (erro)=>{
                    return res.status(400).json({
                        erroStatus: true,
                        erroMessagem: 'Houve um erro ao cadastrar a categoria',
                        erroBancoDados: erro
                    });
        }
    );

});


router.get('/listarCategoria', (req, res)=>{
    modelCategoria.findAll()
        .then(
            (categorias)=>{
                return res.status(200).json(categorias);
            }
        ).catch(
            (erro)=>{
                return res.status(400).json({
                    erroStatus: true,
                    erroMessagem: 'Erro ao selecionar os dados de categoria',
                    erroBancoDados: erro
                });
            }
        );
});

router.put('/alterarCategoria', (req, res)=>{
    let {id, NOME_CATEGORIA} = req.body;

    modelCategoria.update(
        {NOME_CATEGORIA},
        {where:{id}}
    ).then( ()=>{
        return res.status(200).json({
            erroStatus: false,
            menssagemStatus: 'Categoria alterada com sucesso!'
        });
    }).catch(
        (erro)=>{
            return res.status(400).json({
                erroStatus: true,
                erroMessagem: 'Houve um erro ao alterar a categoria',
                erroBancoDados: erro
            });
        }
    );
});

router.delete('/excluirCategoria/:id', (req, res)=>{
    let {id} = req.params;

    modelCategoria.destroy(
        {where: {id}}
    ).then( ()=>{
        return res.status(200).json({
            erroStatus: false,
            menssagemStatus: 'Categoria excluida com sucesso!'
        });
    }).catch(
        (erro)=>{
            return res.status(400).json({
                erroStatus: true,
                erroMessagem: 'Houve um erro ao excluir a categoria',
                erroBancoDados: erro
            });
        }
    );
});

module.exports = router;