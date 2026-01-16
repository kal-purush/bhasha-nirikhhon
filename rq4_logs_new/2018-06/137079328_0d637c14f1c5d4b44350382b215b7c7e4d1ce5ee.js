module.exports = function (app) {
   // this is the actual API POST that will send an email with form data from the dom to the creator of the event.

    app.post("/contact", function (req, res) {
        var api_key = 'key-9b7e5c8f456d2e2af3a4ca35a1ef9a95-47317c98-35df1b95';
        var domain = 'sandbox5220afbde8c34d7b823a5aee1c709219.mailgun.org';
        var mailgun = require('mailgun-js')({
            apiKey: api_key,
            domain: domain
        });
        var data = {
            from: 'Market to Market <postmaster@sandbox5220afbde8c34d7b823a5aee1c709219.mailgun.org>',
            to: "tobross@gmail.com",
            HTML: req.body.vendor+"\n"+req.body.product+"\n"+req.body.vendorEmail+"\n"+req.body.text
        };

        mailgun.messages().send(data, function (error, body) {
            // console.log(data);
            if (!error) {
                res.send("Mail Sent!");
            } else {
                console.log(error);
            }
        });
    });

}