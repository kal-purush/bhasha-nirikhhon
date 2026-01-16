import * as theia from '@theia/plugin';
import LebabTransform from './lebab-transform'

export function start(context: theia.PluginContext) {
    const transform = new LebabTransform()
    context.subscriptions.push(theia.workspace.onDidSaveTextDocument(e => {
        console.info(e);
        transform.onSave(e);
    }))
}

export function stop() {

}
