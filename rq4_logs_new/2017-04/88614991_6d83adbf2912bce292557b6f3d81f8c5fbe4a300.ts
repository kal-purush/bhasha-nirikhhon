import User from '../api/user/user.model';
import config from '../config/config'
import * as jwt from 'jsonwebtoken';

export default {
    signin(req, res) {
        const {email, password} = req.body;
        User.findOne({email: email})
            .exec((err, user) => {
                if(err) {
                    console.log('err', err);
                    return res.status(401).json({status: false, data: null, error: err});
                }
                if(!user) {
                    return res.status(404).json({status: false, data: null, error: 'User not found'});
                }
               
                if(user.validatePassword(password)) {
                    const currentUser = {
                        id: user._id,
                        name: user.name,
                        email: user.email,
                        role: user.role
                    };
                    const token = jwt.sign(currentUser, config.jwtSecret);
                    currentUser['token'] = token;
                    req['user'] = currentUser;
                    res.status(200).json({success: true, data: currentUser, error: null });
                }
                else {
                    res.status(401).json({success: false, data: null, err: 'Invalid credentials'})
                }
            })
    }
};