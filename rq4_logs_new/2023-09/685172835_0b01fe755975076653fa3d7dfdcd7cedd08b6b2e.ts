// this function is used to send an OTP verification to the user's email to verify when they register with their email
import { response } from 'express';
import * as fs from 'fs';
import jwt from 'jsonwebtoken';

// function to generate the OTP code
const generateOTP = async (): Promise<string> => {
    // Define the length of the OTP
    const otpLength = 6;

    // Generate a random OTP with the specified length
    const otp: number = Math.floor(100000 + Math.random() * 900000);

    const string_otp = String(otp);
    // Ensure the OTP has the desired length by padding with zeros if necessary
    //@ts-ignore
    const paddedOTP: string = string_otp.padStart(otpLength, '0');

    return paddedOTP;
}

const sendEmail = async (user_email: string, pickup_code: string, request_type: string, new_password: any = null, user_id: any = null): Promise<any> => {
    const sgMail = require('@sendgrid/mail'); // using twilio sendgid to verify email
    sgMail.setApiKey(process.env.TWILIO_SENDGRID_API_KEY);

    // the path to all the email template files
    const postTemplatePath = 'src/utils/template/twilio_post_template.html';
    const patchTemplatePath = 'src/utils/template/twilio_patch_template.html';
    const deleteTemplatePath = 'src/utils/template/twilio_delete_template.html';

    let template: any = null;

    // read in the HTML template file
    if (request_type == "post") { // template for user to verify their registered email
        template = fs.readFileSync(postTemplatePath, 'utf8');
    } else if(request_type == 'patch') {
        template = fs.readFileSync(patchTemplatePath, 'utf8');
    } else {
        template = fs.readFileSync(deleteTemplatePath, 'utf8');
    };

    // generate a JWT token that stores the user email address and the random OTP for 10 minutes
    const userInformation = {
        "email" : user_email,
        "pickup_code" : pickup_code,
        "user_id" : user_id,
        "new_password" : new_password,
    };

    const token = jwt.sign(userInformation, process.env.JWT_SECRET_KEY, {expiresIn: '10m'});
    
    // Replace the place holder with the actual token
    template = template.replace('{{token}}', token);

    const message = {
        to: userInformation['email'], // recipient
        from: 'pickup123organization@gmail.com',
        subject: 'Verify your email address with PickUp',
        html: template,
    };

    // sending email
    try {  
        const response = await sgMail.send(message);
        console.log('Email sent');
        console.log(response);

        // Resolve the response data
        const responseData = {
            message: "Email sent successfully",
            status: response[0].statusCode,
            token: token,
        };

        return responseData;
    } catch(err) {
        console.error(err);
        throw err; // Re-throw the error to handle it elsewhere
    }
}

export { generateOTP, sendEmail };