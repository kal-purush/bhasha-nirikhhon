/*
   
Copyright 2010, Moritz Stefaner

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
   
 */

module eu.stefaner.elasticlists {

	import ContentItem = eu.stefaner.elasticlists.data.ContentItem;
	import DataConnector = eu.stefaner.elasticlists.data.DataConnector;
	import Facet = eu.stefaner.elasticlists.data.Facet;
	import GeoFacet = eu.stefaner.elasticlists.data.GeoFacet;
	import Model = eu.stefaner.elasticlists.data.Model;
	import DefaultGraphicsFactory = eu.stefaner.elasticlists.ui.DefaultGraphicsFactory;
	import ContentArea = eu.stefaner.elasticlists.ui.appcomponents.ContentArea;
	import DetailView = eu.stefaner.elasticlists.ui.appcomponents.DetailView;
	import ContentItemSprite = eu.stefaner.elasticlists.ui.contentitem.ContentItemSprite;
	import FacetBox = eu.stefaner.elasticlists.ui.facetboxes.FacetBox;
	import FacetBoxContainer = eu.stefaner.elasticlists.ui.facetboxes.FacetBoxContainer;
	import ElasticListBox = eu.stefaner.elasticlists.ui.facetboxes.elasticlist.ElasticListBox;
	import GeoFacetBox = eu.stefaner.elasticlists.ui.facetboxes.geo.GeoFacetBox;

	import HBox = com.bit101.components.HBox;
	import VBox = com.bit101.components.VBox;

	import Logger = org.osflash.thunderbolt.Logger;

	import Sprite = flash.display.Sprite;
	import StageAlign = flash.display.StageAlign;
	import StageScaleMode = flash.display.StageScaleMode;
	import Event = flash.events.Event;
	import TextField = flash.text.TextField;

	/**
	 * App
	 *
	 * Main application class, associated with flash stage.
	 * Creates Model, DataConnector, DetailView etc., and  manages main application states
	 *
	 * For new applications, subclass and override as needed
	 * 
	 * @langversion ActionScript 3.0
	 * @playerversion Flash 10
	 * @version 1.0
	 *
	 * @author moritz@stefaner.eu
	 */
	export class App extends Sprite {

		/**
		 * Manages data and facet information 
		 */
		public model : Model;
		/**
		 * provides connectivity to external data 
		 */
		public dataConnector : DataConnector;
		/**
		 * displays the result set
		 */
		public contentArea : ContentArea;
		/** display details for selected contentitem
		 * 
		 */
		public detailView : DetailView;
		// events

		/** 
		 * dispatched when all facets are loaded and inited
		 * TODO: revisit - really needed?
		 */
		//
		public static FACETS_CHANGED : string = "FACETS_CHANGED";
		/**
		 * disptached when (visible or all) contentitems have changed
		 */
		public static CONTENTITEMS_CHANGED : string = "CONTENTITEMS_CHANGED";
		/**
		 * disptached when facet values (or their stats) are changed 
		 */
		public static FILTERS_CHANGED : string = "FILTERS_CHANGED";
		protected vBox : VBox;
		protected margin : number = 15;
		protected hBox : HBox;
		protected titleTextField : TextField;

		/**
		 * constructor, calls @see startup
		 */
		constructor() {
			this.startUp();
		}

		/** 
		 * initialize @see model and @see dataConnector, starts loading data
		 */
		protected startUp() : void {
			this.initStage();
			this.model = this.createModel();
			this.dataConnector = this.createDataConnector();
			this.dataConnector.addEventListener(DataConnector.DATA_LOADED, this.onDataLoaded);
			this.loadData();
		}

		/**
		 * set scale mode, alignment and add resize listener
		 */
		protected initStage() : void {
			this.stage.scaleMode = StageScaleMode.NO_SCALE;
			this.stage.align = StageAlign.TOP_LEFT;
			this.stage.addEventListener(Event.RESIZE, this.onResize);
		}

		/**
		 * Resize event handler
		 */
		protected onResize(e : Event) : void {
			this.layout();
		}

		/**
		 * start loading data, calls @see eu.stefaner.elasticlists.data.DataConnector.loadData
		 */
		protected loadData() : void {
			this.dataConnector.loadData();
		}

		/**
		 * start loading data, calls @see eu.stefaner.elasticlists.data.DataConnector.loadData
		 */
		protected onDataLoaded(e : Event) : void {
			Logger.info("App.onDataLoaded");
			this.initDisplay();
			this.model.updateGlobalStats();

			this.dispatchEvent(new Event(App.FACETS_CHANGED));
			this.dispatchEvent(new Event(App.CONTENTITEMS_CHANGED));
			this.applyFilters();
		}

		protected initDisplay() : void {
			// create facet boxes etc
			this.vBox = new VBox(this, this.margin, this.margin);
			this.vBox.spacing = this.margin;

			this.titleTextField = DefaultGraphicsFactory.getTitleTextField();
			this.titleTextField.scaleX = this.titleTextField.scaleY = 2;
			if (this.loaderInfo.parameters.appTitle) {
				this.title = this.loaderInfo.parameters.appTitle;
			} else {
				this.title = this._title;
			}

			this.vBox.addChild(this.titleTextField);

			this.hBox = new HBox(this.vBox, 0, 0);
			this.hBox.spacing = this.margin * .33;

			for each (var facet:Facet in this.model.facets) {

				var f : FacetBoxContainer = new FacetBoxContainer(this);
				var facetBox : FacetBox;
				if (facet instanceof GeoFacet) {
					facetBox = new GeoFacetBox();
				} else {
					facetBox = new ElasticListBox();
				}

				f.init(facet, facetBox);
				this.hBox.addChild(f);
				f.width = 180 + 180 * Number(facet instanceof GeoFacet);
				f.height = 200;
			}

			this.contentArea = new ContentArea();
			this.contentArea.init(this);
			this.vBox.addChild(this.contentArea);

			this.layout();
		}

		protected layout() : void {
			this.hBox.draw();
			this.vBox.draw();

			this.contentArea.width = this.stage.stageWidth - this.margin * 2;
			this.contentArea.height = this.stage.stageHeight - this.contentArea.getBounds(this).top - this.margin ;
		}

		/*
		 * creates and returns the dataConnector
		 */
		protected createDataConnector() : DataConnector {
			return new DataConnector(this.model);
		}

		/*
		 * creates and returns the model
		 */
		protected createModel() : Model {
			return new Model(this);
		}

		public createContentItem(id : string) : ContentItem {
			return new ContentItem(id);
		}

		/*
		 * called by ContentArea to get a sprite for a contentItem
		 */
		//
		public createContentItemSprite(contentItem : ContentItem) : ContentItemSprite {
			return new ContentItemSprite(contentItem);
		}

		/*
		 * called by FacetBoxContainer.onSelectionChange
		 * TODO: use events?
		 */
		public applyFilters() : void {

			this.model.applyFilters();

			for each (var facet:Facet in this.model.facets) {
				facet.calcLocalStats();
			}

			this.dispatchEvent(new Event(App.FILTERS_CHANGED));

			if (!this.contentArea) return;

			if (this.contentArea.selectedContentItem && this.contentArea.selectedContentItem.filteredOut) {
				this.contentArea.selectedContentItem.selected = false;
			}

			this.showDetails(this.contentArea.selectedContentItem);
		}

		/** 
		 * show details for selected ContentItem
		 * called by ContentArea when item is clicked
		 */
		public showDetails(selection : ContentItem = null) : void {
			if (this.detailView) {
				this.detailView.display(selection);
			}
		}

		private _title : string = "Elastic Lists";

		public get title() : string {
			return this._title;
		}

		public set title(title : string) {
			this._title = title;
			if (this.titleTextField) this.titleTextField.text = title;
		}
	}
}