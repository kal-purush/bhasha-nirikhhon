module jDebug {
    export class JDebugScriptManager {

        constructor(private resourceManager:jasper.areas.IResourceManager) {

        }

        updateScript(src:string, onDone?:Function) {
            var scriptTag = document.querySelector('script[src^="' + src + '"]');
            if (!scriptTag) {
                console.warn('Script tag with src "' + src + '" not found in the DOM');
                return;
            }
            scriptTag.parentNode.removeChild(scriptTag);

            var noCacheSrc = src + '?v=' + jDebug.makeRandomId();
            this.resourceManager.makeAccessible([noCacheSrc], [], onDone || angular.noop);
        }

    }
}