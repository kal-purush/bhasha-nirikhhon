import {ContextSecurityInterface, ContextSettings} from './context.interface';

export class Context implements ContextSecurityInterface {
	settings: ContextSettings;

	permissionDex: Map<string, boolean>;

	constructor(settings: ContextSettings) {
		this.permissionDex = null;

		this.settings = settings;
	}

	hasPermission(permission: string) {
		if (!this.permissionDex) {
			// lazy load this
			this.permissionDex = new Map<string, boolean>(
				this.settings.permissions.map((permission) => [permission, true])
			);
		}
		return this.permissionDex.has(permission);
	}

	async hasClaim(/*claim: string*/) {
		return false;
	}
}