const schemas = require('../models/schemas.js')

module.exports = {
    getHome: async (req, res) => {
        let menu = schemas.menu
        let sesh = req.session

        let menuResults = await menu.find({})
        .then((menuData) => {
            res.render('index', {title: "Menu Tracker", data: menuData, search: '', loggedIn: sesh.loggedIn})
        })
    }
    getSearch:
}