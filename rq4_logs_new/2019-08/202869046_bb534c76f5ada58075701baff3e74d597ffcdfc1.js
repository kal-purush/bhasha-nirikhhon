const inquirer = require('inquirer')
const fs = require('fs')
const path = require('path')

const replacer = require('./utility/replacer')

module.exports = (templatesDir) => {
    const templatesArr = fs.readdirSync(path.join(__dirname, templatesDir))

    inquirer
	.prompt([{
		type: 'list',
		name: 'template',
		message: 'What would you like to generate?',
		choices: templatesArr
	},
	{
		name: 'name',
		message: 'How would you like to name it?'
	}])
	.then(answers => {
        const templateDir = path.join(__dirname, templatesDir, answers.template)
        const templateConfig = require(path.join(templateDir, 'template.json'))
        const templateFiles = fs.readdirSync(templateDir)

        templateFiles.map(filename => {
            if (filename != 'template.json') {
				const template = fs.readFileSync(path.join(templateDir, filename), 'utf-8')

				switch (templateConfig.type) {
					case 'folder':
						templateTarget = path.join(__dirname, templateConfig.dir, answers.name)
						break;
	
					case 'file':
						templateTarget = path.join(__dirname, templateConfig.dir)
						break;
				
					default:
						console.error('Unknown type')
						break;
				}

				fs.mkdirSync(templateTarget, {recursive:true})
				fs.writeFileSync(
                    path.join(templateTarget,replacer(filename, answers.name)),
                    replacer(template, answers.name),
                'utf-8')
			}
        })
	})
}