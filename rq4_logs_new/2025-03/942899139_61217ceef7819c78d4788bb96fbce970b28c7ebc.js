const UsuarioService = require('../Service/usuarioService.js');

class UsuarioController {
    static async getAll(req, res) {
        const usuarios = await UsuarioService.getAll();
        res.json(usuarios);
    }

    static async create(req, res) {
        const { nome } = req.body;
        const novoUsuario = await UsuarioService.create(nome);
        res.status(201).json(novoUsuario);
    }

    static async update(req, res) {
        const { id } = req.params;
        const { nome } = req.body;
        await UsuarioService.update(id, nome);
        res.json({ id, nome });
    }

    static async delete(req, res) {
        const { id } = req.params;
        await UsuarioService.delete(id);
        res.json({ message: 'Usuário deletado com sucesso' });
    }
}

module.exports = UsuarioController;