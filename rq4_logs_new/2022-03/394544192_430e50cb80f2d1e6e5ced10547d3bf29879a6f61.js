const Skill = require('./models/skillSchema')
const slugify = require('slugify')
const fs = require('fs')
const path = require('path')
const mongoose = require('mongoose')
const updateServer = require('./utils/updateServer')
const dotenv = require('dotenv')

dotenv.config({ path: './.env' })

const db = process.env.DB_CONNECTION.replace(/<PASSWORD>/, process.env.DB_PW)
	.replace(/<DB_NAME>/, process.env.DB_NAME)
	.replace(/<DB_USER>/, process.env.DB_USER)

mongoose
	.connect(db, {
		useNewUrlParser: true,
		useCreateIndex: true,
		useUnifiedTopology: true,
		useFindAndModify: false,
	})
	.then(
		() => console.log('database connected successfully'),
		err => console.log(err)
	)

const directory = process.argv[2]
const files = fs.readdirSync(directory)

const promises = files.map(async (file, i) => {
	const parsed = JSON.parse(
		fs.readFileSync(path.resolve(__dirname, directory, file))
	)
	const slug = slugify(parsed.name, { lower: true })

	const doc = await Skill.findOne({ name: parsed.name })
	if (!doc) {
		const newDoc = await Skill.create({
			...parsed,
			slug,
		})

		return newDoc
	}
})

Promise.all(promises).then(async () => {
	await updateServer()
	console.log('uploaded new files')
	process.exit(0)
})