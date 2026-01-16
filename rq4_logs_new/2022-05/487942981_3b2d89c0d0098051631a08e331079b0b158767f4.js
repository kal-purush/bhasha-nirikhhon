const https = require("https");
const options = {
    hostname: "jose-B450-AORUS-PRO",
    port: 8080,
    path: "/sd",
    method: "GET",
    rejectUnauthorized: false, // Para que admita certificados autofirmados
    requestCert: true,
}

const req = https.request(options, res => {
    console.log(`statusCode: ${res.statusCode}`)

    res.on("data", d => {
        process.stdout.write(d)
    })
})

req.on("error", error => {
    console.log(error)
})

req.end()