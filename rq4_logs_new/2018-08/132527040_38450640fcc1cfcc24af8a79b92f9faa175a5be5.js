'use strict'
const Database = use('Database')
const data = use('App/Utils/Data')
class Admin {
    async list({view,request, response}) {
        
        return view.render('blog/admin/list',  {});
    }
}
module.exports = Admin 