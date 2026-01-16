(function ($) {

    if (!("FormData" in window)) {
        console.error("quickMultiFileUploader: FormData is not supported in you browser");
    }
    else {
        var internalPrefix = '_quickMultiFileUploader';

        $.fn.extend({
            quickMultiFileUploader: function (options) {
                this.each(function () {
                    if (options === undefined) {
                        options = {};
                    }

                    if (options && typeof (options) == 'object') {
                        options = $.extend({}, options);
                    }

                    new $.quickMultiFileUploader(this, options);
                });
            }
        });

        $.quickMultiFileUploader = function (elem, options) {
            if (options && typeof (options) == 'string') {
                if (options == 'upload') {
                    startUpload();
                } else {
                    console.error("quickMultiFileUploader: " + options + " method not recognized");
                }
            }
            else if (!$(elem).is('input') || $(elem).attr('type') != 'file') {
                console.error("quickMultiFileUploader: this plugin is only valid for input file elements");
            }
            else if (!elem.hasAttribute("name")) {
                console.error("quickMultiFileUploader: element must have name attribute");
            }
            else {
                //setup
                options = $.extend({}, $.quickMultiFileUploader.defaults, elem.dataset, options);
                parseDataAttributes(options);
                $(elem).data(internalPrefix, options);

                if (options.uploadOnChange) {
                    $(elem).on('change', function () {
                        startUpload();
                    });
                }
            }

            function startUpload() {
                if (elem.files.length > 0) {
                    var options = $(elem).data(internalPrefix);

                    if (!$.inArray("url", options)) {
                        console.error("quickMultiFileUploader: the url must be set");
                    }
                    else {

                        var elemName = $(elem).attr('name');
                        var data = new FormData();

                        for (var i = 0; i < elem.files.length; i++) {
                            data.append(elemName, elem.files[i], elem.files[i].name);
                        }

                        var customData = $.extend({}, options.data);

                        for (var dataKey in customData) {
                            data.append(dataKey, customData[dataKey]);
                        }
                        console.log("ajax");
                        console.log($.extend({}, options, { data: data }));
                        $.ajax($.extend({}, options, { data: data }))
                            .always(function () {
                                afterUpload();
                            }).fail(function () { console.log(4);});
                    }
                }
            }

            function afterUpload() {
                elem.value = "";
            }

            function parseDataAttributes(dataAttributes) {
                for (var key in dataAttributes) {
                    if (dataAttributes[key] === "") {
                        dataAttributes[key] = true;
                    } else if (dataAttributes[key] == "false") {
                        dataAttributes[key] = false;
                    } else if (dataAttributes[key] == "true") {
                        dataAttributes[key] = true;
                    }
                }
            }
        };


        $.quickMultiFileUploader.defaults = {
            method: "post",
            processData: false,
            contentType: false,
            uploadOnChange: false
        };

        $(document).ready(function () {
            $(".quickMultiFileUploader").quickMultiFileUploader();
        });
    }

}(jQuery));