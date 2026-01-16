module Common {
    interface CommandDictionary {
        [command: string]: CommandFunction;
    }

    export interface CommandFunction {
        (...rest: string[]): string;
    }

    export class Commands {
        private _commands: CommandDictionary = {
            bot_time: function () {
                return (new Date()).toString();
            },
            commands: () => {
                return Object.keys(this._commands).join(', ');
            },
            report_issue: function () {
                return 'https://github.com/mcw8d/ChatBotExtensions/issues/new';
            },
            version: () => {
                return this.version;
            }
        }

        constructor (private version: string) {}

        has(command: string): boolean {
            return this._commands.hasOwnProperty(command);
        }

        get(command: string): CommandFunction {
            return this._commands[command];
        }
    }
}