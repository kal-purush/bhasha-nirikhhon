import {
    SLIDER_TEMPLATE,
    LOCK_BAR_TEMPLATE,
    LOOP_BAR_TEMPLATE,
    PLAY_BAR_TEMPLATE,
    REFRESH_BAR_TEMPLATE,
    DELAY_BAR_TEMPLATE,
    EXPORT_BAR_TEMPLATE
} from './template';

import { SliderControls } from './slider-controls';
import { SliderBar } from './slider-bar';

import { take } from 'rxjs/internal/operators/take';

export default class RangeSlider {
    /**
    * Plugin init
    * @function init
    * @param {Any} mapApi the viewer api
    */
    init(mapApi: any) {
        this.mapApi = mapApi;

        // create panel
        this.panel = this.mapApi.panels.create('rangeSlider');
        this.panel.element.css(RangeSlider.prototype.panelOptions);
        this.panel.body = SLIDER_TEMPLATE;

        // get slider configuration then add/merge configuration for range, limit and delay
        this.config = this._RV.getConfig('plugins').rangeSlider; 
        this.extendConfig = {...RangeSlider.prototype.layerOptions, ...this.config.params};
        this.extendConfig.language = this._RV.getCurrentLang();

        // get array of id(s) and set layer(s)
        const ids = this.config.layers.map(layer => layer.id);
        this.mapApi.layersObj.layerAdded.subscribe((layer: any) => this.setLayer(layer, this.config.layers, ids));
    }

    /**
     * Set layer properties for added layer
     * @function setLayer
     * @param {Any} mapApi the viewer api
     * @param {Any} config the slider configuration
     */
    setLayer(layer: any, config: any, ids: string[]): void {

        // If it is the right layer, get the attributes
        if (ids.indexOf(layer.id) !== -1) {
            const layerInfo = config.find(i => i.id === layer.id);
            this.extendConfig.layers.push(layerInfo);
            this.extendConfig.nbLayers += 1;

            // If layer is ESRI, get all attributes to define the limit
            const layerType = layer.type;
            if ((layerType === 'esriDynamic' || layerType === 'esriFeature') && this.extendConfig.nbLayers === this.config.layers.length) {
                const attrs = layer.getAttributes();

                if (attrs.length === 0) {
                    // make sure all attributes are added before creating the slider
                    this.mapApi.layers.attributesAdded.pipe(take(1)).subscribe(attrs => {
                        if (attrs.attributes.length > 0) {

                            // get attributes value for specified field
                            const values = [];
                            for (let row of attrs.attributes) {
                                values.push(row[layerInfo.field]);
                            }

                            // set limit and range if not set from configuration
                            const limits: Range = { min: Math.min.apply(null, values), max: Math.max.apply(null, values) };
                            if (this.extendConfig.limit.min === null) { this.extendConfig.limit = limits; }
                            if (this.extendConfig.range.min === null) { this.extendConfig.range = limits; }
                        }

                        this.setSliderBar();
                    });
                }
            } else if (layerType === 'ogcWms' && this.extendConfig.nbLayers === this.config.layers.length) {
                // everything must be set inside configuration (range and limit)
                this.setSliderBar();
            }
        }
    }

    /**
     * Set slider bar
     * @function setSliderBar
     */
    setSliderBar(): void {
        // set step
        this.extendConfig.step = (this.extendConfig.range.max - this.extendConfig.range.min);

        // initialiaze slider bar
        this.slider = new SliderBar(this.mapApi, this.extendConfig);

        // set bar controls then open the panel
        this.setBarControls(this.config.controls);
        this.panel.open();
    }

    /**
     * Set slider bar controls
     * @function setBarControls
     * @param {String[]} the array of controls to initialize
     */
    setBarControls(controls: string[]): void {
        // set templates to initialize
        const templates = [
            PLAY_BAR_TEMPLATE
        ];

        // add controls from configuration
        for (let ctrl of controls) {
            if (ctrl === 'lock') { templates.unshift(LOCK_BAR_TEMPLATE); }
            else if (ctrl === 'loop') { templates.push(LOOP_BAR_TEMPLATE); }
            else if (ctrl === 'refresh') { templates.push(REFRESH_BAR_TEMPLATE); }
            else if (ctrl === 'delay') { templates.push(DELAY_BAR_TEMPLATE); }
            else if (ctrl === 'export') { templates.push(EXPORT_BAR_TEMPLATE); }
        }

        // create slider bar controls
        this.panel.controls = new SliderControls(this.mapApi, this.panel, templates, this.slider);
    }
}

export interface Range {
    min: number,
    max: number
}

export default interface RangeSlider {
    mapApi: any,
    _RV: any,
    translations: any,
    panel: any,
    config: any,
    extendConfig: any,
    panelOptions: any,
    layerOptions: any,
    slider: any
}

RangeSlider.prototype.panelOptions = {
    top: 'calc(100% - 165px)',
    height: '110px',
    'margin-right': '60px',
    'margin-left': '420px'
};

RangeSlider.prototype.layerOptions = {
    precision: 2,
    delay: 3000,
    lock: false,
    loop: false,
    export: false,
    range: { min: null, max: null },
    limit: { min: null, max: null },
    layers: [],
    nbLayers: 0
};

RangeSlider.prototype.translations = {
    'en-CA': {
        bar: {
            lock: 'Lock left anchor',
            unlock: 'Unlock left anchor',
            loop: 'Animate in loop',
            unloop: 'Do not animate in loop',
            previous: 'Previous',
            play: 'Play',
            pause: 'Pause',
            foward: 'Next',
            delay: 'Delay',
            refresh: 'Refresh',
            gif: 'GIF',
            tooltip: {
                gif: 'If enabled, click \"Play\" to start then \"Pause\" to export GIF'
            }
        }
    },

    'fr-CA': {
        bar: {
            lock: 'Verrouiller la molette gauche',
            unlock: 'Déverrouiller la molette gauche',
            loop: 'animer en boucle',
            unloop: 'Ne pas animer en boucle',
            previous: 'Précédent',
            play: 'Jouer',
            pause: 'Pause',
            foward: 'Prochain',
            delay: 'Délai',
            refresh: 'Rafraîchir',
            gif: 'GIF',
            tooltip: {
                gif: 'Si activé, cliquez sur \"Jouer\" pour démarrer, puis sur \"Pause\" pour exporter le GIF'
            }
        }
    }
};

(<any>window).rangeSlider = RangeSlider;