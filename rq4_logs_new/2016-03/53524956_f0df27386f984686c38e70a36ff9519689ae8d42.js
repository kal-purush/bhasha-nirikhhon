define([
    'app/utils',
    'text!./ui-datetime.html',
    'app/viewModels/sandbox',
    'globalize',
    'globalize.extensions',
    "DateTime/CommonDateTimeDefaults",
    "DateTime/dateEntry/ko.datePickerBinding",
    "DateTime/dateTimeEntry/ko.dateTimePickerBinding",
    "DateTime/timeEntry/ko.timeEntryBinding",
    "DateTime/ReadOnlyDatePickerBindingHandler"
], function(utils, template, sandbox, Globalize) {
    
    var dateTimeSandbox = new Class({
        Extends: sandbox,
        template: 'template-ui-datetime',
        initData: function () {

            for (var lang in Globalize.cultures) {
                var name = lang == 'default' ? 'Default Language' : Globalize.cultures[lang].nativeName;
                this.data.cultures.push({ value: lang, text: name });
            }

            this.data.changeMonth = ko.observable(true);
            this.data.changeYear = ko.observable(true);
            this.data.showButtonPanel = ko.observable(true);
            this.data.language = ko.observable('default');
            this.data.pickerType = ko.observable('DatePicker');
            this.data.isRange = ko.observable(false);
            this.data.isReadOnly = ko.observable(false);
            this.data.value = ko.observable(new Date());
        },
        initComputed: function () {
            //listen for type and range
            this.data.bindingName = ko.computed(function () {
                var name = this.data.pickerType();
                var range = this.data.isRange();
                if (!this.data.isReadOnly()) {
                    return name.toLowerCase();
                } else {
                    return 'readOnly' + name;
                }
            }.bind(this));
        },
    });

    utils.addTemplates(template);

    return new dateTimeSandbox();
});