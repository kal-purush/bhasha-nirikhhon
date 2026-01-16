import { info } from "@repo/logs/logs";
import { Request, Response, response } from "express";
import { loginService, otpVerificationService, registerService, resendOtpService } from "../services/users.service.js";



export async function registerHandler(req:Request, res : Response) {
    console.log("req body inside registerHandler", req);
    
    const response = await registerService(req);

    return res.send(response);

}

export async function resendOtpHandler(req:Request, res : Response) {
    console.log("req body inside resendOtpHandler", req);
    
    const response = await resendOtpService(req);

    return res.send(response);

}

export async function loginHandler(req:Request, res : Response) {

    info("req body", req);
    const response = await loginService(req);
    return res.send(response);

}

export async function logOutHandler(req:Request, res : Response) {

    const refreshToken = req.cookies?.secretToken2;

    if (!refreshToken) {
        return res.status(400).json({ message: 'No refresh token provided' });
    }

    try {
        // Clear the refresh token cookie
        res.clearCookie('secretToken2', {
            httpOnly: true,
            secure: true,
            sameSite: 'strict', // Prevent cross-site requests
        });

        return res.status(200).json({ message: 'Logged out successfully' });
    } catch (err) {
        console.error('Logout error:', err);
        return res.status(500).json({ message: 'An error occurred during logout' });
    }

}

export async function otpVerificationHandler(req:Request, res : Response) {

    info("req body inside otpVerificationHandler", req);
    const response = await otpVerificationService(req);
    const {success, accessToken, refreshToken} = response;
    console.log("accesstoken", accessToken);
    if(success == "intermediate"){
        res.cookie("secretToken1", accessToken, {
            httpOnly : true,
            secure : process.env.NODE_ENV === "production",
            maxAge : 15 * 60 * 1000
        })
        res.cookie("secretToken2", refreshToken, {
            httpOnly : true,
            secure : process.env.NODE_ENV === "production",
            maxAge : 7 * 24 * 60 * 60 * 1000 
        })

        return res.send({
            success: true,
            message: "You are logged in successfully",
        });
    }
    return res.send(response);

}