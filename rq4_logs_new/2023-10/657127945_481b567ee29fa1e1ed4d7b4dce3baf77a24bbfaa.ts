import { userRegistrationValidation, userLoginValidation } from "../validations/authValidations";
import { UserService } from "../services/user.service";
import { Request, Response } from "express";
import { GeneralUtils } from "../utils/general";
import { sendConfirmationEmail, verificationEmail } from "../utils/mailer";
import { VerificationCodeRepository } from "../repository/VerificationCodeRepository";
import { VerificationCodeService } from "../services/verificationcode.service";
import JwtAuth from "../middlewares/JwtAuth";

const general = new GeneralUtils()
const verificationCodeRepo = new VerificationCodeRepository()

const userService = new UserService() 
const verificationCodeService = new VerificationCodeService(verificationCodeRepo)

class VendorAuth extends JwtAuth{

    createUser = async (req: Request, res: Response) => {
        // Data Validation
        const { error } = userRegistrationValidation(req.body)
        if(error){
            return res.status(400).json({
                status: false,
                message: error.details[0].message.toUpperCase(),
            })
        }
        
        // Check if user exists
        const user = await userService.getOneUser('login',req.body.email);
        
        if(user){
            return res.status(400).json({
                status: false,
                message: 'User already exists',
            })
        }
        
        // create user
        const newUser = await userService.createUser({firstName: req.body.firstName, lastName: req.body.lastName, login: req.body.email,password: req.body.password})
        
        if(!newUser){
            return res.status(500).json({
                status: false,
                message: 'Something went wrong while creating user',
            })
        }
        
        // Generate otp
        const otp = general.generateOtp()
        
        // send otp to email
        const message = '<h2>Hi '+req.body.first_name+' kindly confirm your account by using this otp '+otp
        await general.sendEmail(req.body.email, 'Confirm Account', message);
    
        await sendConfirmationEmail(newUser)
        await verificationCodeService.CreateCode({code: otp, user: newUser._id})
        await verificationEmail(otp, newUser)
        
        return res.status(200).json({
            status: true,
            message: 'User created successfully',
            data: newUser
        })
    
    }
    loginUser = async (req: Request, res: Response) => {
            // Data Validation
            const { error } = userLoginValidation(req.body)
            if(error){
                return res.status(400).json({
                    status: false,
                    message: error.details[0].message.toUpperCase()
                })
            }   
    
            // Check if user exists
            const user = await userService.getOneUser('login',req.body.email);
            if(!user){
                return res.status(401).json({
                    status: false,
                    message: 'Incorrect credentials',
                })
            }else{
                user.matchPassword(req.body.password).then(isMatch => {
    
    
    
                    if(isMatch){
                        return res.status(200).json({
                            status: true,
                            message: "Login successful",
                            user,
                            token: this.createJWT({_id:user._id})
                        })
                    }else{
                        return res.status(401).json({
                            status: false,
                            message: "Incorrect credentials"
                        })
                    }
                }).catch((err) => {
    
                    console.log(err)
                    return res.status(500).json({
                        status: false,
                        message: "Server error occured"
                    })
                })
            }
    }
    
    upgradeToVendor = async (req: Request, res: Response) => {
        // Data Validation
        const { error } = userLoginValidation(req.body)
        if(error){
            return res.status(400).json({
                status: false,
                message: error.details[0].message.toUpperCase()
            })
        }
    
    
        // Check if user exists
        const user = await userService.getOneUser('login',req.body.email);
        if(!user){
            return res.status(401).json({
                status: false,
                message: 'Incorrect credentials',
            })
        }else{
            user.matchPassword(req.body.password).then(isMatch => {
    
    
    
                if(isMatch){
                    return res.status(200).json({
                        status: true,
                        message: "Login successful",
                        user,
                        token: general.generateBearerToken(user._id)
                    })
                }else{
                    return res.status(401).json({
                        status: false,
                        message: "Incorrect credentials"
                    })
                }
            }).catch((err) => {
    
                console.log(err)
                return res.status(500).json({
                    status: false,
                    message: "Server error occured"
                })
            })
        }
    }
}

export default new VendorAuth()






// export default class UserController{
//     constructor(private userService: UserService, private general: GeneralUtils){}

    
//     async createUser(req: Request, res:Response){
//         console.log(this)
//         // Data Validation
//         const { error } = userRegistrationValidation(req.body)
//         if(error){
//             return res.status(400).json({
//                 status: false,
//                 message: error.details[0].message.toUpperCase(),
//             })
//         }

//         console.log("Checking if user exist ")
//         // Check if user exists
//         const user = await this.userService.getOneUser('login',req.body.email);
        
//         if(user){
//             return res.status(400).json({
//                 status: false,
//                 message: 'User already exists',
//             })
//         }

//         // create user
//         const newUser = await this.userService.createUser({firstName: req.body.firstName, lastName: req.body.lastName, login: req.body.email,password: req.body.password})

//         if(!newUser){
//             return res.status(500).json({
//                 status: false,
//                 message: 'Something went wrong while creating user',
//             })
//         }

//         // Generate otp
//         const otp = this.general.generateOtp()

//         // send otp to email
//         const message = '<h2>Hi '+req.body.first_name+' kindly confirm your account by using this otp '+otp
//         await this.general.sendEmail(req.body.email, 'Confirm Account', message);

//         return res.status(200).json({
//             status: true,
//             message: 'User created successfully',
//             data: newUser
//         })

//     }

//     async loginUser(req: Request, res: Response){
//         // Data Validation
//         const { error } = userLoginValidation(req.body)
//         if(error){
//             return res.status(400).json({
//                 status: false,
//                 message: error.details[0].message.toUpperCase()
//             })
//         }


//         // Check if user exists
//         const user = await this.userService.getOneUser('login',req.body.email);
//         if(!user){
//             return res.status(401).json({
//                 status: false,
//                 message: 'Incorrect credentials',
//             })
//         }else{
//             user.matchPassword(req.body.password).then(isMatch => {
//                 if(isMatch){
//                     return res.status(200).json({
//                         status: true,
//                         message: "Login successful",
//                         user,
//                         token: this.general.generateBearerToken(user._id)
//                     })
//                 }else{
//                     return res.status(401).json({
//                         status: false,
//                         message: "Incorrect credentials"
//                     })
//                 }
//             }).catch((err) => {
//                 return res.status(500).json({
//                     status: false,
//                     message: "Server error occured"
//                 })
//             })
//         }
//     }

// }