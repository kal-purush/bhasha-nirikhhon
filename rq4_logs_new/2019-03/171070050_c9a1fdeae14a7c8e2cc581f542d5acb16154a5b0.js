const Mongoose = require('mongoose');

class BebidasService {

    constructor() {

    }

    static conectar() {
        const url = 'mongodb+srv://movileHack:1234@cluster0-emzgx.mongodb.net/test?retryWrites=true';
        Mongoose.connect(url, { useNewUrlParser: true });

        const produtoSchema = new Mongoose.Schema({
            prodId: {
                type: Number,
                required: true
            },
            nome: {
                type: String,
                required: true
            },
            valor: {
                type: String,
                required: true
            },
            eventId: {
                type: Number,
                required: true
            }
        })
        const produtoModel = Mongoose.model('bebidas', produtoSchema);
        return produtoModel
    }

}

module.exports = BebidasService;