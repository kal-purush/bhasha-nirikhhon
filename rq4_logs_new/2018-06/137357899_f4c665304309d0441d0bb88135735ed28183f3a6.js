const User=require('../db/models').user
module.exports={
    insert_user:async(requery)=>{
        console.log(requery)
        User.create({
            user_name:requery.name,
            password:requery.password
        })
    }
}