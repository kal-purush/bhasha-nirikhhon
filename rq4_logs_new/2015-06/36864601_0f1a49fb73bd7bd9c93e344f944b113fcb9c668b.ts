/// <reference path="./Scripts/Actions.ts"/>
/// <reference path="configtypebase.ts" />
module ConfigTypes {
    export class BrokerConfigurations extends ConfigTypeBase {
        actions: Actions;
        page: WebPage;
        phantom: Phantom;
        screenDumpRepository: string;
        addBrokerServerPath: string;
        addBrokerPath: string;
        requiredParameters: { addServer: string[]; addBroker:string[]};

        constructor(phantom: Phantom, screenDumpRepository: string) {
            super();
            this.page = require('webpage').create();
            this.phantom = phantom;
            this.screenDumpRepository = screenDumpRepository;
            this.actions = new Actions(this.page, this.phantom, screenDumpRepository);
            this.addBrokerServerPath = 'wm.apps.msg.brokerservers.server.add';
            this.addBrokerPath = 'wm.app.msg.brokerservers.brokers.addbroker';
        }

        public exitOnTimeout = (step: Step) => {
                step.page.close();
                step.phantom.exit(2);
            }

        login(config: any) {
                console.log(JSON.stringify(config));
                document.querySelector("#wm_login-username").setAttribute('value', config.loginUserName);
                var passwordInputBox: any = document.querySelector("#wm_login-password");
                passwordInputBox.value = config.loginPassword;
                var loginForm: any = document.forms['Login'];
                loginForm.submit();
            }

        focusHostNameInput(config: any) {
                var newServerHtmlForm = document.querySelector('form[id$="newServerHtmlForm"]');
                var hostnameInput: any = newServerHtmlForm.querySelector('input[id$="hostnameInputText"]');
                hostnameInput.focus();
            }

        focusPortInput(config: any) {
            var newServerHtmlForm = document.querySelector('form[id$="newServerHtmlForm"]');
            var portInput: any = newServerHtmlForm.querySelector('input[id$="portInputText"]');
            portInput.focus();
        }

        clickAddServer(config: any) {
            var newServerHtmlForm = document.querySelector('form[id$="newServerHtmlForm"]');
            var addButton: any = newServerHtmlForm.querySelector('button[id$="addCommandButton"]');
            var ev = document.createEvent("MouseEvents");
            ev.initEvent("click", true, true);
            addButton.dispatchEvent(ev);
        }

        pickServer(config: any) {
            console.log('starting pickServer');
            var filterOptions = (options: any, value: any) => {
                for (var i = 0; i < options.length; i++) {
                    if (options[i].value.trim() == value) { return i; }
                } return -1;
            }
        var selectServerMenu: any = document.querySelector("select[id$='SelectServerMenu']");
            var serversOption: any = selectServerMenu.options;
            serversOption.selectedIndex = filterOptions(serversOption, config.serverName + ':' + config.port);
            console.log('completed pickServer');
        }

        focusBrokerNameInput(config: any) {
            var newBrokerForm = document.querySelector('form[id$="defaultForm"]');
            var brokerNameInput: any = newBrokerForm.querySelector('input[id$="BrokerNameText"]');
            brokerNameInput.focus();
        }

        focusBrokerDescriptionInput(config: any) {
            var newBrokerForm = document.querySelector('form[id$="defaultForm"]');
            var brokerDescriptionInput: any = newBrokerForm.querySelector('input[id$="BrokerDescriptionText"]');
            brokerDescriptionInput.focus();
        }

        clickAddBroker(config: any) {
            var newBrokerForm = document.querySelector('form[id$="defaultForm"]');
            var addButton: any = newBrokerForm.querySelector('button[id$="addButton"]');
            var ev = document.createEvent("MouseEvents");
            ev.initEvent("click", true, true);
            addButton.dispatchEvent(ev);
        }

        noop(config: any) {
            console.log('noop');
        }

        noPrerequisite(config: any) {
            return true;
        }

        end = (step: Step) => {
            console.log('end in 5 seconds');
            var page = step.page;
            var phantom = step.phantom;
            setTimeout(function () {
                page.close();
                phantom.exit(0);
            }, 5000);
        }

        public addServerSetup(config: any) {
            // verify config
            var missingConfig = [];
            var phantom = this.phantom;
            var exitOnTimeout = this.exitOnTimeout;
            var addBrokerServerPath = this.addBrokerServerPath;
            var noPrerequisite = this.noPrerequisite;
            var requiredParameterList = this.requiredParameters.addServer;

            this.actions
                .withConfig(config, requiredParameterList)
                .open(config.url, 'open main page')
                .then(this.login, 'login', (config: any) => { return document.querySelector("#wm_login-username") != null; }, this.exitOnTimeout, 1000, 20000)
                .then(this.noop, 'sleep 2secondsAfterLogin', (config: any) => { return false; }, this.noop, 2000, 2000)
                .open(config.url + '/' + addBrokerServerPath, 'open addBrokerServer page', (config: any) => { return document.querySelector('#header > div > div > div.banner > table > tbody > tr > td.banner-links') != null; }, this.exitOnTimeout)
                .then(this.focusHostNameInput, 'focusHostNameInput', () => { return document.querySelector('form[id$="newServerHtmlForm"]') != null; }, this.exitOnTimeout, 500, 10000)
                .sendEvent('keypress', config.serverName, 'enter host name')
                .then(this.focusPortInput, 'focusPortInput', noPrerequisite, this.exitOnTimeout, 500, 10000)
                .sendEvent('keypress', config.port.toString(), 'enter port')
                .then(this.noop, 'sleep 1secondsAfterBrokerServerEntry', (config: any) => { return false; }, this.noop, 1000, 1000)
                .then(this.clickAddServer, 'click add server', noPrerequisite, this.exitOnTimeout, 500, 10000)
                .end(this.end);
        }

        public addBrokerSetup(config: any) {
            var phantom = this.phantom;
            var exitOnTimeout = this.exitOnTimeout;
            var addBrokerPath = this.addBrokerPath;
            var noPrerequisite = this.noPrerequisite;
            var requiredParameterList = this.requiredParameters.addBroker;
            // todo:may be refactor the steps - 
            this.actions
                .withConfig(config, requiredParameterList)
                .open(config.url, 'open main page')
                .then(this.login, 'login', (config: any) => { return document.querySelector("#wm_login-username") != null; }, this.exitOnTimeout, 500, 10000)
                .then(this.noop, 'sleep 2seconds', (config: any) => { return false; }, this.noop, 2000, 2000)
                .open(config.url + '/' + addBrokerPath, 'open addBroker page', (config: any) => { return document.querySelector('#header > div > div > div.banner > table > tbody > tr > td.banner-links') != null; }, this.exitOnTimeout)
                .then(this.pickServer, 'pickServer', (config: any) => { return document.querySelector("select[id$='SelectServerMenu'] option") != null }, this.exitOnTimeout, 500, 10000)
                .then(this.focusBrokerNameInput, 'focusBrokerNameInput', noPrerequisite, this.exitOnTimeout, 500, 10000)
                .sendEvent('keypress', config.brokerName, 'enter broker name')
                .then(this.focusBrokerDescriptionInput, 'focusBrokerDescriptionInput', noPrerequisite, this.exitOnTimeout, 500, 10000)
                .sendEvent('keypress', config.description, 'enter description')
                .then(this.noop, 'sleep 1secondsAfterBrokerEntry', (config: any) => { return false; }, this.noop, 1000, 1000)
                .then(this.clickAddBroker, 'click add broker', noPrerequisite, this.exitOnTimeout, 10000, 10000)
                .end(this.end);
        }

        public configure() {
            this.actions.run();
        }

    }
    BrokerConfigurations.prototype.requiredParameters = {
        addServer: ['loginUserName', 'loginPassword', 'url', 'serverName', 'port'],
        addBroker: ['loginUserName', 'loginPassword', 'url', 'serverName', 'port', 'brokerName', 'description']
    };

}