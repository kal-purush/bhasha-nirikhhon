const express = require('express')
const bodyParser = require('body-parser')
var Razorpay = require('razorpay')

const { mongoose } = require('./connect')
const { User } = require('./user.js')
var rzp = new Razorpay({
    key_id: 'own_key',// need to change in payment.ejs
    key_secret: 'own_secret'
})

if (typeof localStorage === "undefined" || localStorage === null) {
    var LocalStorage = require('node-localstorage').LocalStorage;
    localStorage = new LocalStorage('./scratch');
}

const msg91sms = require('msg91-lib').msg91SMS;
const msg91SMS = new msg91sms('309320AUiZd9175dfd9491P1', 'incand', 4, 91);

const sgMail = require('@sendgrid/mail');
sgMail.setApiKey('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');

var app = express()

app.set('view engine', 'ejs')
app.use(express.static('public'))



app.use(bodyParser.urlencoded({ extended: true }))

//Seeding

// var seedDB= function(){
//     User.remove({},function(err){
//         if(err){
//             console.log(err);
//         }
//     });
// }
// seedDB();


// start of get request
app.get('/',(req, res)=>{
    res.render('landing')
})

app.get('/register', (req, res) => {
    res.render('register', { validate: 1 })
})

app.get('/adminlogin', (req, res) => {
    res.render('adminlogin')
})

app.get('/payment', (req, res) => {
    name = req.query.name
    phone = req.query.phonenumber
    email = req.query.email

    localStorage.setItem('email', email)
    localStorage.setItem('name', name)
    localStorage.setItem('number', phone)
    res.render('payment')
    //{name:name,phonenumber:phone,email:email}
})

app.get('/success', (req, res) => {
    var name = localStorage.getItem("name") || req.query.name;
    res.render('success', { name: name })
    console.log(localStorage.getItem("name"))
    localStorage.clear()
})

app.get('/admin', (req, res) => {
    res.render('admin')
})

//End of get request

//Start of post request
//to valididate email id
function fun1(req, res, next) {
    var email = req.body.email
    var pos = email.indexOf('@')

    if (pos == -1) {
        res.render('register', { validate: 0 })
    }
    else {
        if (email.slice(pos + 1, email.length) == "gmail.com" || email.slice(pos + 1, email.length) == "yahoo.com") {
            next()
        } else {
            res.render('register', { validate: 0 })
        }
    }
}

app.post('/register', fun1, (req, res) => {
    college_id = req.body.college
    if (college_id == "1") {
        phone = req.body.number
        if (phone.length != 10) res.redirect('/register')
        else {
            new User({
                name: req.body.name,
                email: req.body.email,
                institute: "NIT Silchar",
                phonenumber: phone
            }).save().then((user) => {
                console.log(user)
                res.redirect('/success?name=' + req.body.name)
            })
        }
    } else {
        res.redirect('/payment?name=' + req.body.name + '&email=' + req.body.email + '&phonenumber='
            + req.body.number + '&institute=others')
    }

})

app.post('/adminlogin', (req, res) => {
    username = req.body.username
    password = req.body.password
    if (username == "admin" && password == "password") {
        res.redirect('/admin')
    } else {
        res.redirect('/adminlogin')
    }
})

app.post('/pay', (req, res) => {
    console.log(req.body)
    const amount = 1000
    name = localStorage.getItem('name')
    email = localStorage.getItem('email')
    phone = localStorage.getItem('number')
    console.log(localStorage.getItem("name"))
    console.log(localStorage.getItem("email"))
    console.log(localStorage.getItem("number"))

    rzp.customers.create({
        name: name,
        email: email,
        fail_existing: '0'
    }).then((data) => {
        console.log(data)
        rzp.orders.create({ amount: amount, currency: "INR", receipt: data.id }).then((order) => {
            console.log("Order Details: ");
            console.log(order);
            console.log(order.id)
            var orderID = order.id;
            rzp.payments.fetch(req.body.razorpay_payment_id).then((response) => {
                console.log("Payment Instance")
                console.log(response)
                rzp.payments.capture(req.body.razorpay_payment_id, response.amount).then((capture) => {
                    new User({
                        name: name,
                        email: email,
                        institute: "Others",
                        phonenumber: phone
                    }).save().then((user) => {
                        console.log(user)
                        res.redirect('/success')
                    })
                    console.log("Payment Captured Successfully: ")
                    console.log(capture)
                }).catch((error) => {
                    console.log("Failed to capture payment")
                    console.log(error)
                })
            }).catch((error) => {
                console.log("failed to fetch payment")
                console.log(response)
            })
        }).catch((error) => {
            //error
            console.log("Order creation error")
            console.log(error)
            res.redirect('/register')
        })
    }).catch((error) => {
        // error
        console.log("Customer Create Error: ");
        console.log(error);
        res.redirect('/register')
    })

})


app.post('/send', (req, res) => {
    message = req.body.message
    User.find().then((users) => {
        users.map((user) => {
            string = user.phonenumber
            phnumber = string
            console.log(phnumber)
            
            smsobj = [{
                "message": message,
                "to": [phnumber]
            }]

            args = { sender: 'incand', sms: smsobj }

            msg91SMS.send(args).then((res) => {
                console.log(res)
            }).catch((e) => {
                console.log(e)
                if (e.data) console.log(e.data)
                else console.log(e.message)
            })
            console.log('success')
        })

        users.map((mail_user)=>{
            const msg = {
                to: 'abinashdtt45@gmail.com',
                from: 'incand@gmail.com',
                subject: 'Incandescence News Update',
                text: message,

            };
            msg.to = mail_user.email
            sgMail.send(msg);

        })

    })

})



//End of post request



app.listen(3000, () => {
    console.log('Connected to server')
})








