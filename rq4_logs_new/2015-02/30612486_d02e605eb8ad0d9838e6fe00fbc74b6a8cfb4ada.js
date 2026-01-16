define(['knockout', 'jquery', './async-click-state'], function(ko, $, asyncClickState) {
    'use strict';

    //TODO: (?) Suporter scénario comme https://github.com/knockout/knockout/blob/master/src/binding/defaultBindings/event.js
    //http://knockoutjs.com/documentation/event-binding.html
    ko.bindingHandlers['asyncClick'] = {
        'init': function(element, valueAccessor, allBindings, viewModel, bindingContext) {
            var handlerFunction = valueAccessor() || {};

            ko.utils.registerEventHandler(element, 'click', function(event) {
                var handlerReturnValue;
                if (!handlerFunction || asyncClickState.asyncTask())
                    return;

                try {
                    // Take all the event args, and prefix with the viewmodel
                    var argsForHandler = ko.utils.makeArray(arguments);
                    viewModel = bindingContext['$data'];
                    argsForHandler.unshift(viewModel);
                    handlerReturnValue = handlerFunction.apply(viewModel, argsForHandler);

                    if ($.isFunction(handlerReturnValue)) {
                        asyncClickState.asyncTask(handlerReturnValue);
                        handlerReturnValue.always(function() {
                            asyncClickState.asyncTask(null);
                        });
                    }
                } finally {
                    //Par convention...
                    event.preventDefault();
                }

                var bubble = allBindings.get('asyncClickBubble') !== false;
                if (!bubble) {
                    event.cancelBubble = true;
                    if (event.stopPropagation) {
                        event.stopPropagation();
                    }
                }
            });
        }
    };
});