const express = require('express')
const mongoose = require('mongoose')
const Prog = require('./models/prog')
const Ile = require('./models/iletisim')
const adminRoutes = require('./routes/adminRoutes')
const app = express()

const dbURL = 'mongodb+srv://nr:jkQ5YdukNPvxf8A1@s-e.harbb.mongodb.net/?retryWrites=true&w=majority'
mongoose.connect(dbURL, { useNewUrlParser: true, useUnifiedTopology: true})
    .then((result) => {
        app.listen(8080)
    }).catch((err) => {
        console.log(err);
    });

app.set('view engine','ejs')


app.use(express.static('public'))
app.use(express.urlencoded({ extended: true }))

app.get('/XlukSqQYYUHurRS7XPnCJGTXHQ1PeIBUdHc1y1oIANC7d6grnc5Qi2Uu6D0wtpJRWLKaVA5cLiJfp6CibdYPRVXU8G5vnmRzg28phnR4v2C3ZIeTtElFFp5iK2MmSv2O0LEhox1MyOxPlRqyB5L8wt',(req,res) => {
    res.render('add',{title:'Yeni ekle'})
})

const createToken = (id) => {
    return jwt.sing({id}, 'nr', {expiresIn: maxAge})
}

app.get('/',(req,res) => {
    Prog.find().sort({ createdAt: -1})
        .then((result) => {
            res.render('index',{title:'Anasayfa',progs: result})   
        }).catch((err) => {
            console.log(err);
        });
})

app.post('/' , (req , res)=>{

    const baslik = (req.body)
    console.log(baslik);

    Prog.findOne({baslik}, (err, doc) => {
        if (err) {
            console.error(err)
        } else {
            console.log(doc);
            res.render('ara',{title: 'Arama sonuçları',ara: doc})
        }
      })    
})

const id = ''

Prog.findOne({id}, (err, doc) => {
  if (err) {
    console.error(err)
  } else {
    console.log(doc);
  }
})

app.get('/qXnk5p2rkk7djSkxAIZAfbXrEjuaexmqYRPvhSX0LFN2yDtC0E0QnUvGqC27KNdTFAwZGglO1ZY',(req,res) => {
    Ile.find().sort({ createdAt: -1})
        .then((result) => {
            res.render('msj',{title:'Mesajlar',ile: result})   
        }).catch((err) => {
            console.log(err);
        });
})

app.use(adminRoutes)

app.get('/iletisim',(req,res) => {
    res.render('iletisim',{title:'İletişim'})
})

app.get('/hakkimizda',(req,res) => {
    res.redirect('/about',{title:'Hakkımızda'})
})


app.get('/:id',(req,res) => {
    const id = req.params.id
    Prog.findById(id)
        .then((result) => {
            res.render('indir',{ prog: result, title: 'İndir'})
        }).catch((err) => {
            res.status(404).render('404',{title:'Hata'})
        });
})


app.use((req,res) => {
    res.status(404).render('404',{title:'404'})
})