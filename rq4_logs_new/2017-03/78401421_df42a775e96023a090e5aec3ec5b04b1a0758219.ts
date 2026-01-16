import * as path from "path";
import { Answers } from "inquirer";
import { ICommand, ICommandOptions } from "./command";

import { promptUse } from "../lib/prompt";
import Project from "../lib/project";
import { instance as config } from "../lib/config";
import { instance as logger } from "../lib/logger";
import { Package, instance as pkg } from "../lib/package";

var chalk = require('chalk');

class Use implements ICommand {
  doc ="use [<folder>]";
  requiresPkg = true;
  requiresLogin = false;

  folder: string;
  package: Package;
  constructor() {
  }
  run(options: ICommandOptions) {

    // HELP(ethan)
    //  I feel like I'm using your idioms wrong here..
    //  I want to load the package in cwd or in the specified optional folder
    let basePath = pkg.packageRootPath || process.cwd();
    if(options["<folder>"]) {
      basePath = path.resolve(basePath, options["<folder>"]);
    }
    this.package = new Package(basePath);
    promptUse().then(this.provideHelp.bind(this));
  }
  provideHelp(ans: Answers) {  
    var user = this.package.get('user');
    var app = this.package.get('app');
    var variant = this.package.get('variant');
    console.log(ans);
    switch (ans['environment']) {
      case 'html':
        console.log(`
${chalk.green.bold('To place this widget on a web page:')}

${chalk.green('1) Paste the following code into the <HEAD> element:')}

<script src="https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/0.7.13/webcomponents-lite.min.js"></script>
<link rel="import" href="https://components.cloudstitch.com/cloudstitch-$VARIANT.html" />

${chalk.green('2) Paste the following code where the widget should appear:')}

<cloudstitch-${variant} user="${user}" app="${app}"></cloudstitch-${variant}>          
                
        `);
        break;
      case 'iframe':
        console.log(`
${chalk.green.bold('Use this HTML to embed this widget as an IFrame:')}

<iframe src="//www.cloudstitch.com/widget/${user}/${app}"></iframe>
                
        `);
        break;
      case 'wordpress':
        console.log(`
${chalk.green.bold('To place this widget in WordPress')}

${chalk.green('1) Install the Cloudstitch WordPress plugin, found here::')}

  https://wordpress.org/plugins/cloudstitch/

${chalk.green('2) Paste the following short code on the post or page ')}
${chalk.green('   where the widget should apper:')}

[cloudstitch container="${variant}" user="${user}" app="${app}"][/cloudstitch]
                
        `);
        break;
      case 'weebly':
        console.log(`
${chalk.green.bold('To place this widget in Weebly, follow the instructions at this URL:')}

  http://docs.cloudstitch.com/publishing/weebly/?user=${user}&app=${app}&variant=${variant}
                
        `);
        break;
      case 'squarespace':
        console.log(`
${chalk.green.bold('To place this widget in Squarespace, follow the instructions at this URL:')}

  http://docs.cloudstitch.com/publishing/squarespace/?user=${user}&app=${app}&variant=${variant}
                
        `);
        break;
      case 'googlesites':
        console.log(`
${chalk.green.bold('To place this widget in Google Sites, follow the instructions at this URL:')}

  http://docs.cloudstitch.com/publishing/google-sites/?user=${user}&app=${app}&variant=${variant}
                
        `);
        break;
      default:
        logger.error("Unknown usage target");
    }
  }
} 

export = new Use();