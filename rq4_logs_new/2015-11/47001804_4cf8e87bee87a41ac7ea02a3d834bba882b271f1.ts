import {GenericConfig} from './generic_config'

export interface BackupProvider {
	supportVersioning():boolean;
	
	getConfigs(mergable:boolean):GenericConfig[];
	getConfig(template:GenericConfig, mergable:boolean):GenericConfig;
	
	save();
}

export function getBackupProvider():BackupProvider {
	return null;
}