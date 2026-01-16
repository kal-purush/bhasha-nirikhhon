/// <reference path="../panel/PagePanel.ts" />
/// <reference path="../AppView.ts" />
/// <reference path="../../typings/jquery.d.ts" />
/// <reference path="AppPageDefinitions.ts" />
/// <reference path="PanelPageOptions.ts" />



module App.Page {

    export class PanelPage extends App.Page.AppPage {
        /**
         * The Max width a panel can have
         * @type {number}
         */
        public static MAX_PANEL_WIDTH : number  = 12;

        private panels : Array<App.Panel.PagePanel>;
        private name : string;
        constructor(PageOptions: App.Page.AppPageOptions) {
            super();
            this.panels = PageOptions.panels;
            this.name = PageOptions.name;
        }

        /**
         * Suspend the appPage includes but not limited to
         * - Unbind events
         * - Run suspend method on children panes
         *
         * Basically we want to make sure this page does not execute in any way
         * if we are not currently looking at it
         */
        suspend(): void {

        }
        /**
         * Resume the Page
         * Includes
         * - Checking that our state is correct
         * - If not, rebuilding based on state
         * - rebinding events
         */
        resume(): void {
            this.AppendPanels();
        }
        render(): App.Page.PanelPage {
            for(var i = 0; i<this.panels.length; i++) {
                var panel : App.Panel.PagePanel = this.panels[i];
                panel.render();
            }
            return this;
        }
        private AppendPanels(): void {
            var currentPanelWidth : number = 0;
            var panelRow : JQuery = this.createNewPanelRow();
            //Append the first row
            this.$el.append(panelRow);
            for(var i = 0; i<this.panels.length; i++) {
                var panel : App.Panel.PagePanel = this.panels[i];
                //Check if the panel is applicable (E.G it has the data it needs
                //or other constraints defined in the panel)
                //maybe we only want to show if its a certain day of the month
                //special data is included...
                if(!panel.isApplicable()){
                    //Nope the panel is not applicable, jump to the next ielement
                    //in our for loop
                    continue;
                }
                var panelWidth : number = panel.getPanelWidth();
                if(panelWidth>App.Page.PanelPage.MAX_PANEL_WIDTH)  {
                    console.warn("[APPPAGE:Append]Panel size invalid (Too high),\
                        you might experience wierd layout (like one row to many)");
                }
                //Check if we have exeeded the max panel width
                if(currentPanelWidth+panelWidth>App.Page.PanelPage.MAX_PANEL_WIDTH) {
                    //Create a new row and reset current width
                    panelRow = this.createNewPanelRow();
                    currentPanelWidth = 0;
                    //Append new row
                    this.$el.append(panelRow);
                } else {
                    //Add to current width
                    currentPanelWidth += panelWidth;
                }
                //Append the panel to our row
                //Add correct class
                var columnClass : string = " col-md-"+panelWidth+" ";
                panel.$el.addClass(columnClass);
                //Append
                panelRow.append(panel.$el);
            }
        }
        private updatePanelState(): void {

        }
        private createNewPanelRow(): JQuery {
            return $('<div></div>').addClass('row');
        }
    }
}