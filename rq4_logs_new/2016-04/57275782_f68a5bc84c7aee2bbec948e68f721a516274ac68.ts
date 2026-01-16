var view = require("ui/core/view");
import proxy = require("ui/core/proxy");
import observable = require("data/observable");
var dependencyObservable = require("ui/core/dependency-observable");
import * as builderModule from "ui/builder";
import * as observableArrayModule from "data/observable-array";
import * as weakEventsModule from "ui/core/weak-event-listener";

var observableArray: typeof observableArrayModule;
function ensureObservableArray() {
    if (!observableArray) {
        observableArray = require("data/observable-array");
    }
}

var builder: typeof builderModule;
function ensureBuilder() {
    if (!builder) {
        builder = require("ui/builder");
    }
}

var ITEMS = "items";
var ITEMTEMPLATE = "itemTemplate";
var RECYCLERVIEW = "RecyclerView";

export module knownTemplates {
    export var itemTemplate = "itemTemplate";
}

function onItemsPropertyChanged(data: dependencyObservable.PropertyChangeData) {
    var recyclerView = <RecyclerView>data.object;
    recyclerView._onItemsPropertyChanged(data);
}

function onItemTemplatePropertyChanged(data: dependencyObservable.PropertyChangeData) {
    var recyclerView = <RecyclerView>data.object;
    recyclerView.refresh();
}

export class RecyclerView extends view.View {
    public static itemsProperty = new dependencyObservable.Property(
        ITEMS,
        RECYCLERVIEW,
        new proxy.PropertyMetadata(
            undefined,
            dependencyObservable.PropertyMetadataSettings.AffectsLayout,
            onItemsPropertyChanged
            )
        );
   
     public static itemTemplateProperty = new dependencyObservable.Property(
        ITEMTEMPLATE,
        RECYCLERVIEW,
        new proxy.PropertyMetadata(
            undefined,
            dependencyObservable.PropertyMetadataSettings.AffectsLayout,
            onItemTemplatePropertyChanged
            )
        );
        
        get items(): any {
            return this._getValue(RecyclerView.itemsProperty);
        }
        set items(value: any) {
            this._setValue(RecyclerView.itemsProperty, value);
        }
        
        get itemTemplate(): string | view.Template {
            return this._getValue(RecyclerView.itemTemplateProperty);
        }
        set itemTemplate(value: string | view.Template) {
            this._setValue(RecyclerView.itemTemplateProperty, value);
        }
        
        public refresh() {
        }
    
        public _onItemsPropertyChanged(data: dependencyObservable.PropertyChangeData) {
            ensureObservableArray();
            
            this.refresh();
        }

        private _onItemsChanged(args: observable.EventData) {
            this.refresh();
        }   
}