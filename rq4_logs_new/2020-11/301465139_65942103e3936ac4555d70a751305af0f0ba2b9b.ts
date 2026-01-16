import Handlebars from "handlebars";

import * as argoCommands from "./argoCommands";

const prOpenedSource: string = `Hi, I'm a bot that helps with Kubernetes deployments. Invoke me via \`botCommand\` on PRs.

* :mag: To show a diff for a specific app, comment:
    * \`{{ BotActions.Diff }} {{ BotDiffActions.App }} <app_name>\`
* :ledger: To show a diff for all apps, comment:
    * \`{{ BotActions.Diff }} {{ BotDiffActions.App }}\`
* :question: To show full help for argo, comment:
    * \`{{ BotActions.Help }}\`

`;

const template = Handlebars.compile(prOpenedSource);

export let prOpenedComment: string = template(argoCommands);