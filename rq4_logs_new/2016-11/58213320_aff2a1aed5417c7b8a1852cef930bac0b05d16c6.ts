import './entrypoint-selector.html!';
import 'bower_components/vaadin-combo-box/vaadin-combo-box.html!'

@component('entrypoint-selector')
class EntrypointSelector extends polymer.Base {

    @property({notify: true, type: String, readOnly: true})
    url:string;

    @property({readOnly: true})
    apis:Array;

    attached() {
        var apis = Polymer.dom(this).children.map(apiEl => {
            return {
                label: apiEl.textContent,
                value: apiEl.getAttribute('data-url')
            };
        });

        this._setApis(apis);
    }

    _entrypointSelected(e) {
        this._setUrl(e.detail.value);
    }
}

EntrypointSelector.register();