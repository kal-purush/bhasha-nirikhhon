const authService = require('@services/authService');
const UserRoles = require('@models/userRoles')
exports.showLogin = (req, res) => {
    res.render('auth/login', {layout:'auth'});
};
exports.doLogin = async (req, res) => {
    const {email, password} = req.body;
    const user = await authService.login(email, password);
    if(!user){
        req.flash('errors',['ایمیل یا کلمه عبور معتبر نمی باشد']);
        return res.redirect('/auth/login');
    }
    req.session.user = user;
    const pathToRedirect = user.role === UserRoles.ADMIN ? '/admin/dashboard' : '/';
    return res.redirect(pathToRedirect);

};
exports.showRegister = (req, res) => {
    res.newRender('auth/register', {layout:'auth'});
};
exports.doRegister = async (req, res) => {
    const {email, password, password_confirmation} = req.body;
    const newUserId = await authService.register(email, password);
    console.log(newUserId);
    if(!newUserId){
        req.flash('errors', ['در حال حاضر امکان ثبت نام شما وجود ندارد']);
        return res.redirect('/auth/register');
    }
    return res.redirect('/auth/login');
};