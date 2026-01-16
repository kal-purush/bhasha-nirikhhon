/// <reference path='../typings/node/node.d.ts'/>
/// <reference path='../typings/angularjs/angular.d.ts'/>

module SyncPlaylist {
    var fs = require('fs');

    export class Settings {
        public sourceDirectory: string;
        public targetDirectory: string;

        public persist(): void {
            var toWrite = JSON.stringify({
                sourceDirectory: this.sourceDirectory,
                targetDirectory: this.targetDirectory
            });
            fs.writeFile(this.getSettingsLocation(), toWrite, { encoding: 'utf-8' }, function(err) {
                if (err) throw err;
            });
        }

        public load(): void {
            var settings = this;

            fs.readFile(this.getSettingsLocation(), { encoding: 'utf-8' }, function(err, data) {
                if (err) throw err;

                var parsed = JSON.parse(data);

                settings.sourceDirectory = parsed.sourceDirectory;
                settings.targetDirectory = parsed.targetDirectory;
            });
        }

        private getSettingsLocation(): string {
            return process.cwd() + '/sync-playlist.json';
        }
    }

    angular.module('syncPlaylist.services', [])
        .service('settings', Settings);
}