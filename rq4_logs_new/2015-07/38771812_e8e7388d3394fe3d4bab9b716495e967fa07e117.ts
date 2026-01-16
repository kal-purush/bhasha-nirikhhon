/// <reference path="../core/Module.ts" />
/// <reference path="../views/editor/layout/EditorContainer.ts" />

module App.Modules {
    export class Editor extends App.Module {

        private editorContainerUI: App.View.Editor.Layout.EditorContainer;

        constructor() {
            super();
        }
        appReady(): void {
            this.editorContainerUI = new App.View.Editor.Layout.EditorContainer();

            this.context.getMessengerInstance().subscribe(
                App.constants["MESSAGES"]["FULLVIEWREADY"],
                _.bind(this.onFullViewReady,this)
            );
        }
        private onFullViewReady(message: App.Messaging.Message) {
            this.renderUI();
        }
        private renderUI(): void {
            this.editorContainerUI.render();

            $('.full-view-page').append(this.editorContainerUI.$el);
            this.editorContainerUI.postRender();
        }
    }
}