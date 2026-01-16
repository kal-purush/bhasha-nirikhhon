import userService from '../services/user-service.js'
import {ResultFul} from '../middlewares/result-process';
import ConstantUtils from '../middlewares/ConstantUtils'
import regeneratorRuntime from '../node_modules/regenerator-runtime/runtime.js';

exports.login = async function (req,res) {
    let username = req.query.username;
    let password = req.query.password;
    let token = req.query.token;
    try {
        let retInfo = await userService.login(username,password,token)
        ResultFul.success(retInfo,res)
    }catch (err) {

    }
}