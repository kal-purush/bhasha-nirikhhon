module.exports = {
    ensureBusinessOwner: function(req, res, next) {
        errors = []
        if ((req.user.usertype === true)) {
            return next();
        }
        errors.push({
            msg: "Error, your user type does not allow this action",
        });
        res.render("index", {errors, title: "Dashboard", currentUser: req.user})
    },
    ensureCustomer: function(req, res, next) {
        errors = []
        if ((req.user.usertype === false)) {
            return next(); 
        }
        errors.push({
            msg: "Error, your user type does not allow this action",
        });
        res.render("index", {errors, title: "Dashboard", currentUser: req.user})
    }
  }