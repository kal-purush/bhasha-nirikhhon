/// <reference path="../../typings/tsd.d.ts" />
interface Window {
    port: any;
}

interface KeyboardEventInit {
    keyCode: number;
}

module ChatBot {
    interface CommandFunction {
        (...rest: string[]): string;
    }

    interface CommandDictionary {
        [command: string]: CommandFunction;
    }

    class LocalCommands {
        private _commands: CommandDictionary = {
            'bot_time': function () {
                return (new Date()).toString();
            },
            commands: () => {
                return Object.keys(this._commands).join(', ');
            }
        }

        has(command: string): boolean {
            return this._commands.hasOwnProperty(command);
        }

        get(command: string): CommandFunction {
            return this._commands[command];
        }
    }

    export class Relay {
        private _chatContainer = $('.hN.so.Ij').children(':nth-child(2)');
        private _commandRegEx = /^#\!(\w+)/;
        private _inputDiv = $('.vE.dQ.editable');
        private _localCommands = new LocalCommands();
        private _observerConfig: MutationObserverInit = {
            childList: true
        };
        private _currentChatObserver: MutationObserver;
        private _newChatObserver: MutationObserver;

        constructor (private _cid: string) {
            var _this = this;
            this._currentChatObserver = new MutationObserver(function (mutations) {
                mutations.forEach(function (mutation) {
                    for (var i = 0; i < mutation.addedNodes.length; i++) {
                        var addedNode = $(mutation.addedNodes[i]);
                        if (addedNode.hasClass('Mu SP')) {
                            var message = addedNode.children(':last-child').text();
                            var command = _this.getCommand(message);
                            if (command) {
                                _this.emitResponse(command());
                            }
                        }
                    }
                });
            });
            this._newChatObserver = new MutationObserver(function (mutations) {
                mutations.forEach(function (mutation) {
                    for (var i = 0; i < mutation.addedNodes.length; i++) {
                        var addedNode = $(mutation.addedNodes[i]);
                        if (addedNode.hasClass('tk Sn') && addedNode.is(':last-child')) {
                            var chatBlock = addedNode.find('.JL');
                            if (chatBlock.length === 1) {
                                var message = chatBlock.children().children(':last-child').text();
                                var command = _this.getCommand(message);
                                if (command) {
                                    _this.emitResponse(command());
                                }
                                _this._currentChatObserver.disconnect();
                                _this._currentChatObserver.observe(chatBlock.get(0), _this._observerConfig);
                            }
                        }
                    }
                });
            });
            this._newChatObserver.observe(this._chatContainer.get(0), this._observerConfig);
            this._currentChatObserver.observe(this._chatContainer.children(':last-child').find('.JL').get(0), this._observerConfig);
        }

        close () {
            this._newChatObserver.disconnect();
            this._currentChatObserver.disconnect();
        }

        getCommand (message: string): CommandFunction {
            var regexResults = this._commandRegEx.exec(message);
            if (regexResults && regexResults.length > 1 && this._localCommands.has(regexResults[1])) {
                return this._localCommands.get(regexResults[1]);
            }
            return undefined;
        }

        emitResponse (response: string) {
            this._inputDiv.append(document.createTextNode(response));
            this._inputDiv.get(0).dispatchEvent(
                new KeyboardEvent('keypress', {key: 'Enter', keyCode: 13})
            );
        }
    }
}

window.setTimeout(function () {
    if (document.body.getAttribute('class') === 'nN') {
        var cidObserverConfig: MutationObserverInit = {
            attributes: true,
            attributeFilter: ['cid']
        };
        var cidObserver = new MutationObserver(function (mutations) {
            mutations.forEach(function (mutation) {
                if (mutation.target instanceof Element) {
                    var chatBot = new ChatBot.Relay((<Element>mutation.target).getAttribute('cid'));
                    self.port.emit('NewRelay', chatBot);
                    cidObserver.disconnect();
                }
            });
        });

        cidObserver.observe(document.body, cidObserverConfig);
    }
}, 1000);