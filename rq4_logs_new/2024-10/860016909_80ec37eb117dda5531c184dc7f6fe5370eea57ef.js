import { extname } from "path"
import Employe from "../models/Employe.js"
import User from "../models/User.js"
import bcrypt from 'bcrypt'
import multer from "multer"


const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, "public/uploads")
    },
    filename: function (req, file, cb) {
        cb(null, Date.now() + '-' + path.extname(file.originalname))
    },
})

const upload = multer({ storage: storage })
const ajoutEmploye = async (req, res) => {
    try{
        const {
            nom,
            prenom,
            email,
            employeId,
            dateNaissance,
            sexe,
            statutMatrimonial,
            designation,
            departement,
            salaire,
            password,
            role,
        } = req.body

        const user = await User.findOne({ email })
        if (user) {
            return res.status(400).json({ success: false, error: 'Email déjà utilisé!' })
        }
        const hashedPassword = await bcrypt.hash(password, 10)
        const newUser = new User({
            nom: nom,
            prenom: prenom,
            email: email,
            password: hashedPassword,
            role: role,
            profileImage: req.file? req.file.filename : "",
        })
        const saveUser = await newUser.save()

        const newEmploye = new Employe({
            userId: saveUser._id,
            employeId,
            dateNaissance,
            sexe,
            statutMatrimonial,
            designation,
            departement,
            salaire
        })
        await newEmploye.save()
       return res.status(200).json({ success: true, message:"Employé créé avec succes !!"})
    }catch(error) {
        console.error("Erreur lors de l'ajout de l'employé :", error.message)
        return res.status(500).json({ success: false, error: 'Erreur lors de l\'ajout de l\'employé !!' })
    }
}

const updateEmploye = async (req, res) => {

}

const deleteEmploye = async (req, res) => {

}

const getEmployes = async (req, res) => {

}

const getEmployeById = async (req, res) => {

}

export {upload, ajoutEmploye, updateEmploye, deleteEmploye, getEmployes, getEmployeById };