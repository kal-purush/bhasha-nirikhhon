const mongoose = require('mongoose');
const Schema = mongoose.Schema;
const bcrypt = require('bcrypt');

const AlunoSchema = new Schema({
    name: {type: String, required: true, min: 1, max: 100 },
    email: {type: String, unique: true, required: true, lowercase: true},
    password: {type: String, required: true, select: false},
    state: {type: String},
    city: {type: String},
    telephone:{
        ddd:{type: String, max:3},
        numero:{type: String, min: 1, max: 100}
    },
    school_name: {type: String },
    created_at: {type: String, default: Date.now},
    updated_at: {type: Date}

});

AlunoSchema.pre('save', async function(next){
    let aluno = this;
    if(!aluno.isModified('password')) return next();
    
    aluno.password = await bcrypt.hash(aluno.password, 10);
    return next();

});

module.exports = mongoose.model('Aluno', AlunoSchema);