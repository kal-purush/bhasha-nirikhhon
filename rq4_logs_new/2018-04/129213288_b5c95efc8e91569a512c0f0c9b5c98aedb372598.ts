/*
 * Copyright (C) 2018 Red Hat, Inc. and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */

import yosay = require('yosay');
import Base = require('yeoman-generator');
module.exports = class TheiaPlugin extends Base {

    private params!: {
        author: string
        version: string
        license: string
        pluginName: string
        pluginType: string
        githubURL: string
        extensionPrefix: string
        example: boolean
        theiaVersion: string
        pluginSourcePath: string
        pluginDistPath: string
        isFrontend: boolean
        isBackend: boolean
    };

    constructor(args: string | string[], options: any) {
        super(args, options);
        this.argument('pluginName', {
            type: String,
            required: false,
        });

        this.option('pluginType', {
            alias: 't',
            description: 'Type of the plugin [backend, frontend]',
            type: String,
        });

        this.option('author', {
            alias: 'a',
            description: 'The extension\'s author',
            type: String
        });
        this.option('version', {
            alias: 'v',
            description: 'The plugin\'s version',
            type: String,
            default: '0.0.1'
        });
        this.option('description', {
            alias: 'd',
            description: 'The plugin\'s description',
            type: String,
        });
        this.option('license', {
            alias: 'l',
            description: 'The plugin\'s license',
            type: String
        });
        this.option('githubURL', {
            alias: 'u',
            description: 'The plugin\'s Github URL',
            type: String
        });

        this.option('theia-version', {
            alias: 't',
            description: 'The version of Theia to use',
            type: String,
            default: 'latest'
        });
    }

    path() {
        this.sourceRoot(__dirname + '/../../templates');
    }

    getName() {

    }

    async doPrompt() {

        if (!(this.options as any).pluginName) {
            const answers = await this.prompt([{
                type: 'input',
                name: 'name',
                message: "What is the plugin's name?",
                default: this.appname // Default to current folder name
            }]);
            (this.options as any).pluginName = answers.name
        }

        if (!(this.options as any).pluginType) {

            const answers = await this.prompt([{
                type: 'list',
                name: 'pluginType',
                message: 'What type of extension do you want?',
                choices: [
                    {
                        name: 'Backend plugin, it will run on the server side.',
                        value: 'backend'
                    },
                    {
                        name: 'Frontend extension, it will run on the browser side.',
                        value: 'frontend'
                    }
                ]
            }]);
            (this.options as any).pluginType = answers.pluginType
        }

    }

    prompting() {
        this.log(
            yosay(`Welcome to the 'Theia Plugin' generator!`)
        );

        return Promise.resolve(this.doPrompt());



    }

    configuring() {
        const options = this.options as any
        const pluginName = options.pluginName as string
        const pluginType = options.pluginType as string
        const pluginFullname = pluginName.split('-').map(name => this._capitalize(name)).join('');
        this.params = {
            author: options.author,
            version: options.version,
            license: options.license,
            pluginName: pluginName,
            pluginType: pluginType,
            githubURL: options.githubURL,
            extensionPrefix: pluginFullname,
            example: options.example,
            theiaVersion: options["theia-version"],
            pluginSourcePath: pluginName + '-' + pluginType + '-plugin.ts',
            pluginDistPath: pluginName + '-' + pluginType + '-plugin.js',
            isFrontend: (options.pluginType === 'frontend'),
            isBackend: (options.pluginType === 'backend')
        }
        options.params = this.params

    }

    writing() {
        this.fs.copyTpl(
            this.templatePath('package.json'),
            this.destinationPath('package.json'),
            { params: this.params }
        );
        this.fs.copyTpl(
            this.templatePath('gitignore'),
            this.destinationPath('.gitignore'),
            { params: this.params }
        );
        this.fs.copyTpl(
            this.templatePath('README.md'),
            this.destinationPath('README.md'),
            { params: this.params }
        )
        this.fs.copyTpl(
            this.templatePath('tsconfig.json'),
            this.destinationPath('tsconfig.json'),
            { params: this.params }
        );
        this.fs.copyTpl(
            this.templatePath('index.ts'),
            this.destinationPath('src/' + this.params.pluginName + '-' + this.params.pluginType + '-plugin.ts'),
            { params: this.params }
        );
        if (this.params.isFrontend) {
            this.fs.copyTpl(
                this.templatePath('webpack.config.js'),
                this.destinationPath('webpack.config.js'),
                { params: this.params }
            );
        }
    }

    install() {
        this.spawnCommandSync('yarn', []);
        this.spawnCommandSync('yarn', ['build']);
    }

    private _capitalize(name: string): string {
        return name.substring(0, 1).toUpperCase() + name.substring(1)
    }
}