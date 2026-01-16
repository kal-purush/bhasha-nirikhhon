import * as ko from 'knockout';

export class Contex {
    cityCount = 3;
}

ko.bindingHandlers['dxWeatherForecast'] = {
    init: function(element: HTMLElement, valueAccessor, allBindings, viewModel, bindingContext) {
        var value = ko.unwrap(valueAccessor() || {});
        var entity = new ForeCastModel(value.cityCount);
        var childContext = bindingContext.createChildContext(entity);
        ko.renderTemplate('forecast-content', childContext, {}, element, 'replaceNode');
    }
};

import { ForeCastModel } from './forecastModel';
window.onload = () => ko.applyBindings(new Contex(), document.getElementById('page-wrapper'));