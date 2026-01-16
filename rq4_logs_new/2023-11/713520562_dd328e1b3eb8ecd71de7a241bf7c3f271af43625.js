const AuthSchema = require('../mongodb/models/auth')
const jwt = require('jsonwebtoken')
const bcrypt = require('bcryptjs')

const register = async(req, res)=> {
    try{
        const{username, password, email} = req.body
        const user = await AuthSchema.findOne({ email: email });

        if(user){
            return res.status(500).json({msg: "Böyle bir kullanıcı var"})
        }
        if(password.length <6 ){
            return res.status(500).json({msg: "Şifreniz 6 krakterden küçük olamaz"})
        }
        const passwordHash = await bcrypt.hash(password, 12)

        if (!ValidateEmail(email)) {
            return res.status(500).json({msg: "Email giriniz."})
        }
        const newUser = await AuthSchema.create({username, email, password: passwordHash})

        const token = await jwt.sign({id: newUser._id}, "SERET_KEY", {expiresIn: '1h'})
        res.status(201).json({
            status: "OK ",
            newUser,
            token
        })
    }catch(error){
        return res.status(500).json({ msg: error.message });


    }
}
const Login = async (req, res) => {
  try {
    const {email, password}= req.body
    const user = await AuthSchema.findOne({ email: email });
    if(!user){
         return res.status(500).json({ msg: "Böyle bir kullanıcı bulunamadı" });
    }
    const passwordCompare = await bcrypt.compare(password, user.password)
    if(!passwordCompare){
         return res.status(500).json({ msg: "Şifre yanlış" });
    }
    
    const token = await jwt.sign({ id: user._id }, "SERET_KEY", {
        expiresIn: "1h",
    });
    res.status(201).json({
        status: "OK ",
        user,
        token,
    });
  } catch (error) {
    return res.status(500).json({ msg: error.message });
  }
};






function ValidateEmail(mail) {
  if (/^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$/.test(mail)) {
    return true;
  }
  return false;
}

module.exports = { register, Login}