const express = require("express");
const dotenv = require("dotenv");
const cors = require("cors");

const mg = require("mailgun-js");
dotenv.config();
const mailgun = () =>
	mg({
		apiKey: process.env.MAILGUN_API_KEY,
		domain: process.env.MAILGUN_DOMIAN,
	});

const app = express();
app.use(
	cors({
		accessControlAllowOrigin: "*",
	})
);

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.post("/send", (req, res) => {
	const { email, to, subject, text } = req.body;
	const data = {
		from: email,
		to: to,
		subject: subject,
		text: text,
		html: `<h1>${email} says lets study</h1>`,
	};
	mailgun()
		.messages()
		.send(data, (error, body) => {
			if (error) {
				return res.status(500).json({
					message: error.message,
				});
			}
			return res.status(200).json(body);
		});
});

const port = 5000;
app.listen(port, () => {
	// eslint-disable-next-line no-console
	console.log(`serve at http://localhost:${port}`);
});