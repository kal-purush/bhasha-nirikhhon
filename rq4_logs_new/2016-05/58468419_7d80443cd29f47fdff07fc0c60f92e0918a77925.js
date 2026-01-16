//未登录
function noLogin(req,res,next){
    if(!req.session.user){
        console.log('抱歉，您还没有登录！');
        return res.redirect('/mylogin');//返回登录页面
    }
    next();// pass control to the next handler
}
function alreadyLogin(req,res,next){
    if(req.session.user){
        console.log('已经登录过了。');
        return res.redirect('/list');//返回列表页面
    }
    next();// pass control to the next handler
}
exports.noLogin = noLogin;
exports.alreadyLogin = alreadyLogin;