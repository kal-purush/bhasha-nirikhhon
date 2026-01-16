import {bindable, autoinject} from 'aurelia-framework';
import {LauncherManager} from './launcher-manager';

@autoinject
export class BrowserTile {

    @bindable launcher;
    @bindable platform;

    dataLoaded:boolean = false;
    errors:string;
    manager:LauncherManager;
    icon:any;

    constructor(manager:LauncherManager) {
        this.manager = manager;
    }

    async attached() {
        // Try and download the launcher data file
        try {
            let result = await this.manager.getLauncher(this.platform, this.launcher.path);
            this.dataLoaded = true;
            this.icon = result.image;
        }        
        catch(err) {
            this.errors = err;
            this.dataLoaded = true;
            this.icon = 'images/monterey-logo.png';
        }
    }

    async install() {
        if(this.dataLoaded) {
            if(this.errors) return;

            // Do stuff
            try {
                await this.manager.installLauncher(this.platform, this.launcher.path);
                alert('Launcher installed');
            }
            catch (err) {
                alert(err.message);
            } 
        }
    }
}