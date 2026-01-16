require('dotenv').config();
var fs = require("fs");
const mailgunLoader = require("mailgun-js");
import * as jwt from "jsonwebtoken"
const mailgun = mailgunLoader({
apiKey: process.env.MAILGUN_KEY,
domain: process.env.MAILGUN_DOMAIN,
});
const {
    SECRET
  } = process.env;

const secreto: any = SECRET

export function passwordReset(obj: any) {

  const token = jwt.sign({ userId: obj._id, action: 'password_reset' }, secreto, { expiresIn: 1800 });

  var modelEmail = fs.readFileSync("./src/mailmodel/ResetPass.html", 'utf8', function (err: any, data: any) {
    if (err) console.error(err);
    return data
  })

  var datatemplate = `<a style="padding:0.5em; display:inline-block; text-decoration:none; background-color: red;
   color:#ffffff; margin:.5em; border-radius:.5em;"href="${process.env.ULR_FRONT ||
    'http://localhost:3000'}/newPass/Return/?reset=${token}">RESETEAR CONTRASEÑA</a>`;
  var copypaste = `http://localhost:3000/newPass/Return?reset=${token}`
  modelEmail = modelEmail.replace("%givenname%", obj.name.firstname.toUpperCase());
  modelEmail = modelEmail.replace("%familyname%", obj.name.lastname.toUpperCase());
  modelEmail = modelEmail.replace("%resetlink%", datatemplate);
  modelEmail = modelEmail.replace("%direccion%", copypaste);

  mailgun.messages().send({
    from: "Henry App <SoyHenry@henry.com>",
    to: obj.email,
    subject: 'Cambio de contraseña',
    html: modelEmail
  }, function (err: any, info: any) {
    if (err) {
      console.error('Error: ' + err);
    } else {
      console.error('Response: ' + info);
    }
  });

  return modelEmail;
}