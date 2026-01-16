const express=require('express');
const bodyParser=require('body-parser');
const mongojs=require('mongojs');
const db=mongojs('shop',['pet']);
const {body,param,validationResult}=require('express-validator');
const jwt=require('jsonwebtoken');
const dotenv=require('dotenv').config();
const secret=dotenv.parsed.SECRET;
const users=[
    {username:"Zar",password:"password",role:"admin"},
    {username:"Li",password:"password",role:"admin"},
]
const app=express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
    extended: true
}))
app.get('/api/petrecords',(req,res)=>
    {
        const options = req.query;
        const sort = options.sort || {}
        const filter = options.filter || {}
        const limit = 20;
        const page = parseInt(options.page) || 1;
        const skip = (page - 1) * limit
        for (i in sort) {
            sort[i] = parseInt(sort[i]);
    
        }// for looping is to know increasing order or decreasing order
        db.pet.find(filter).sort(sort).skip(skip).limit(limit, (err, data) => {
            if (err) res.sendStatus(500);
            else {
                return res.status(200).json({
                    meta: {
                        skip,
                        limit,
                        sort,
                        filter,
                        page,
                        total: data.length
                    },
                    data,
                    links: {
                        self: req.originalUrl,
                    }
                })
            }
        })
    })
app.post('/api/login',(req,res)=>
    {
        const {username,password}=req.body;
        const user=users.find((u) =>{
            return u.username===username && u.password===password;
        });
        if(user){
            jwt.sign(user,secret,{
                expiresIn:"1h"
            },(err,token)=>{
                return res.status(200).json({token});
            })
            
        }
        else{
            return res.sendStatus(401);
        }
});
function onlyAdmin(req,res,next){
    const [type,token]=req.headers['authorization'].split(' ');
    if(type!='Bearer') return res.sendStatus(401);
    jwt.verify(token,secret,(err,userdata)=>{
        console.log(userdata)
        
        if (err) return res.sendStatus(401);
        else
           {if(userdata.role==='admin') next();
           else return res.sendStatus(403);
    }
    })
}

app.post('/api/addpet',onlyAdmin,[
    body('name').not().isEmpty(),
    body('age').not().isEmpty(),
    body('color').not().isEmpty(),
    body('eyeColor').not().isEmpty(),
    body('type').not().isEmpty(),
    body('price').not().isEmpty(),
],
    (req, res) => {
        const errors = validationResult(req);
        if (!errors.isEmpty()) {
            return res.status(400).json({ errors: errors.array() });
        }
        db.pet.insert(req.body, (err, data) => {
            if (err) {
                return res.status(500);
            }
            const _id = data._id
            res.append("Location", "/api/records/" + _id);
            return res.status(201).json({
                meta: {
                    _id
                },
                data
            })
        })
})
app.patch('/api/updatepet/:id',onlyAdmin,[
    param('id').isMongoId()],(req, res) => {
    const _id = req.params.id;
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
            return res.status(400).json({ errors: errors.array() });
        }
    console.log(_id);
    db.pet.count({
        _id: mongojs.ObjectID(_id)
    }, (err, count) => {

        if (count) {
            db.pet.update(
                { _id: mongojs.ObjectID(_id) },
                { $set: req.body },
                { multi: false },
                (err, data) => {
                    if(err) return res.send(500);
                    db.pet.find({
                        _id: mongojs.ObjectID(_id)
                    }, (err, data) => {
                        if(err) return res.status(500)
                        return res.status(200).json({
                            meta: { 
                                _id,
                                },
                            data
                        })
                    })
                })
        }
        else {
            return res.sendStatus(404);
        }
    })
})
app.delete('/api/deletepet/:id',onlyAdmin,[
    param('id').isMongoId()],(req,res)=>{
    const _id=req.params.id;
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
            return res.status(400).json({ errors: errors.array() });
        }
    db.pet.count({
        _id:mongojs.ObjectID(_id)
    },(err,count)=>{
        if(count){
            db.pet.remove({
                _id:mongojs.ObjectID(_id)
            },(err,data)=>{
                if(err) res.status(500)
                return res.sendStatus(204);
            })
        }
        else{
            return res.sendStatus(404);
        }
        
        
    
    })
})
app.get('/api/adminview',onlyAdmin, (req, res) => {
    const options = req.query;
    const sort = options.sort || {}
    const filter = options.filter || {}
    const limit = 20;
    const page = parseInt(options.page) || 1;
    const skip = (page - 1) * limit
    for (i in sort) {
        sort[i] = parseInt(sort[i]);

    }// for looping is to know increasing order or decreasing order
    db.pet.find(filter).sort(sort).skip(skip).limit(limit, (err, data) => {
        if (err) res.sendStatus(500);
        else {
            return res.status(200).json({
                meta: {
                    skip,
                    limit,
                    sort,
                    filter,
                    page,
                    total: data.length
                },
                data,
                links: {
                    self: req.originalUrl,
                }
            })
        }
    })
})
    app.listen(8080,()=>{
        console.log('Sever is running with port 8080');
    })