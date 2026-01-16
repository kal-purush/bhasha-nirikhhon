import sgMail from "@sendgrid/mail"

const sendGridAPIKey = process.env.sendGridAPIKey


sgMail.setApiKey(sendGridAPIKey)

// sgMail.send({
//     to: "omarkhalili273@gmail.com",
//     from: "omarkhalili810@gmail.com",
//     subject: "sending hello",
//     text: "hello Omar"
// }).then(() => {
//     console.log('Email sent')
// })
//     .catch((error) => {
//         console.error(error)
//     })


const sendEmail = (email, name, Content) => {
    sgMail.send({
        to: email,
        from: "omarkhalili810@gmail.com",
        subject: "welcoming new  user",
        text: `${Content} ${name}`
    }).then(() => {
        console.log('Email sent')
    })
        .catch((error) => {
            console.error(error)
        })
}


export default sendEmail

