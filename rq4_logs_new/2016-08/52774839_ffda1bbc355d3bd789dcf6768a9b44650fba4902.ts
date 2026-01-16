import * as api from 'twitchr-plugin-api';
import {Plugin} from './pluginManager';

export class HookCollection {
    private actionHooks: Array<api.PluginOnAction> = [];
    private joinHooks: Array<api.PluginOnJoin> = [];
    private messageHooks: Array<api.PluginOnMessage> = [];
    private namesHooks: Array<api.PluginOnNames> = [];
    private partHooks: Array<api.PluginOnPart> = [];

    constructor(plugins: Array<Plugin>) {
        for (let i: number = 0; i < plugins.length; i++) {
            const hooks: api.PluginEventListener = plugins[i].getPlugin().hooks;

            if (hooks.onAction) {
                this.actionHooks.push(hooks.onAction);
            }

            if (hooks.onJoin) {
                this.joinHooks.push(hooks.onJoin);
            }

            if (hooks.onMessage) {
                this.messageHooks.push(hooks.onMessage);
            }

            if (hooks.onNames) {
                this.namesHooks.push(hooks.onNames);
            }

            if (hooks.onPart) {
                this.partHooks.push(hooks.onPart);
            }
        }
    }

    getActionHooks(): Array<api.PluginOnAction> {
        return this.actionHooks;
    }

    getJoinHooks(): Array<api.PluginOnJoin> {
        return this.joinHooks;
    }

    getMessageHooks(): Array<api.PluginOnMessage> {
        return this.messageHooks;
    }

    getNamesHooks(): Array<api.PluginOnNames> {
        return this.namesHooks;
    }

    getPartHooks(): Array<api.PluginOnPart> {
        return this.partHooks;
    }
}