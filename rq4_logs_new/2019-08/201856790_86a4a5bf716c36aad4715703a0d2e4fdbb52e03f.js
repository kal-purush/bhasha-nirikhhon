/**
 * A basic plugin to store annotations on a REST-style HTTP/JSON endpoint.
 */

annotorious.plugin.VanillaREST = (function () {
    'use strict';
    function VanillaREST(options) {

        /** @private **/
        this._annotations = [];

        /** @private **/
        this._loadIndicators = [];


        this.options = {
            extraAnnotationData: {},
            loadFromSearch: false,
            prefix: '/store',
            urls: {
                create: '/annotation',
                read: '/annotations',
                update: '/annotation/:id',
                destroy: '/annotation/:id',
                // search: '/annotations/search'
            }
        };

        this.options = jQuery.extend(this.options, options);
    };


    VanillaREST.prototype.initPlugin = function (anno) {
        var self = this;
        
        anno.addHandler('onAnnotationCreated', function (annotation) {
            self._create(annotation);
            setTimeout(location.reload.bind(location), 500);
            
            // self._loadAnnotations(anno);
            
            // anno._addAnnotations(anno);
            
            
            // self.addAnnotation(annotation);
        });

        anno.addHandler('onAnnotationUpdated', function (annotation) {
            self._update(annotation);
            setTimeout(location.reload.bind(location), 500);
            // self._loadAnnotations(anno);
        });

        anno.addHandler('onAnnotationRemoved', function (annotation) {
            self._delete(annotation);
            setTimeout(location.reload.bind(location), 500);
            // self._loadAnnotations(anno);
        });
        self._loadAnnotations(anno);
        setTimeout(location.reload.bind(location), 120000);
        
    };

    VanillaREST.prototype.onInitAnnotator = function (annotator) {
        
        var spinner = this._newLoadIndicator();
        // console.log(this._loadIndicators)
        // console.log(annotator)
        annotator.element.appendChild(spinner);
        this._loadIndicators.push(spinner);
    };


    /**
     * @private
     */
    VanillaREST.prototype._loadAnnotations = function (anno) {
        // console.log(this)
        // console.log("loading")
        // console.log(this._annotations)
        
        
        var self = this;
        var url = '';
        if (this.options.loadFromSearch === false) {
            url = this._getActionUrl('read', null);
        } else {
            url = this._getActionUrl('search', null);
        }
        // console.log(this._annotations)
        // console.log(data)
        // console.log(this._getAnnotationData)
        // console.log(data)
        jQuery.getJSON(url, function (data) {
            console.log(data)
            data = data["data"]
            console.log(data)
            
            try {
                
                jQuery.each(data, function (index, data) {
                    
                    var annotation = {};
                    if (typeof data['source'] != 'undefined' && typeof data['id'] != 'undefined') {
                        
                        annotation = data['source'];
                        annotation.id = data['id'];
                    } else if (data !== null) {
                        annotation = data;
                    } else {
                        return;
                    }

                    // check for required properties
                    var reqProp = ['src','text','shapes','context'];
                    for (var rp in reqProp) {
                        if (reqProp.hasOwnProperty(rp) && !annotation.hasOwnProperty(reqProp[rp])) {
                            // return;
                        }
                    }
                    // console.log(self._annotations)
                    
                    if (jQuery.inArray(annotation._id, self._annotations) < 0) {
                        // console.log("detected new anno")
                        console.log(annotation._id)
                        self._annotations.push(annotation._id);
                        var myAnnotation = {
                            /** The URL of the image where the annotation should go **/
                            id: annotation["_id"],
                            src : annotation["src"],
                        
                            /** The annotation text **/
                            text : annotation["text"],
                        
                            /** The annotation shape **/
                            shapes : [{
                                /** The shape type **/
                                type : 'rect',
                        
                                /** The shape geometry (relative coordinates) **/
                                geometry : { x : annotation["x"], y: annotation["y"], width : annotation["width"], height: annotation["height"] }
                            }],
                            commenter: annotation["commenter"]
                        }
                        // console.log(annotation.shapes)
                        // if (!annotation.shapes) {
                        
                        anno.addAnnotation(myAnnotation);
                        // console.log("adding annotation")
                        // console.log(myAnnotation)
                        // console.log(self._annotations)
                        // console.log("ok") //}
                        // console.log(anno)
                    }
                });
            } catch (e) {
                self.showNotification(e);
            }

            // Remove all load indicators
            jQuery.each(self._loadIndicators, function (idx, spinner) {
                console.log(spinner)
                jQuery(spinner).remove();
            });
        }).fail(function(jqXHR) {
            self._onResponseError(jqXHR, 'load');
        });
    };


    /**
     * @private
     */
    VanillaREST.prototype._create = function (annotation) {
        var self = this;
        jQuery.post(this._getActionUrl('create', null), this._getAnnotationData(annotation), function (response) {
            // not storing numbers correctly
            console.log(this._getAnnotationData)
            console.log(annotation)
            annotation.id = response['id'];
            
        }).fail(function(jqXHR) {
            self._onResponseError(jqXHR, 'create');
        });
    };

    /**
     * @private
     */
    VanillaREST.prototype._update = function (annotation) {
        var self = this;
        console.log(this._getActionUrl('update', annotation.id))
        jQuery.ajax({
            url: this._getActionUrl('update', annotation.id),
            type: 'PUT',
            data: this._getAnnotationData(annotation)
        }).fail(function(jqXHR) {
            self._onResponseError(jqXHR, 'update');
        });
        jQuery.relo
    };

    /**
     * @private
     */
    VanillaREST.prototype._delete = function (annotation) {
        // var self = this;
        // console.log(annotation)
        jQuery.ajax({
            url: this._getActionUrl('destroy', annotation.id),
            type: 'DELETE'
        }).fail(function(jqXHR) {
            self._onResponseError(jqXHR, 'delete');
        });
    };


    /**
     * @private
     */
    VanillaREST.prototype._newLoadIndicator = function () {
        var outerDIV = document.createElement('div');
        outerDIV.className = 'annotorious-rest-plugin-load-outer';

        var innerDIV = document.createElement('div');
        innerDIV.className = 'annotorious-rest-plugin-load-inner';

        outerDIV.appendChild(innerDIV);
        return outerDIV;
    };

    /**
     * Get url for given action
     * @private
     * @param {string} action
     * @param {int} id
     * @returns {string} returns url for given action
     */
    VanillaREST.prototype._getActionUrl = function (action, id) {
        var url;
        url = this.options.prefix !== null ? this.options.prefix : '';
        url += this.options.urls[action];
        url = url.replace(/\/:id/, id !== null ? '/' + id : '');
        url = url.replace(/:id/, id !== null ? id : '');
        return url;
    };

    VanillaREST.prototype._getAnnotationData = function (annotation) {
        //console.log(annotation)
        var data;
        jQuery.extend(annotation, this.options.extraAnnotationData);
        data = annotation;
        console.log(annotation)
        return data;
    };

    VanillaREST.prototype._onResponseError = function(jqXHR, action) {
        var message = "Sorry we could not " + action + " this annotation";
        if (action === 'search') {
            message = "Sorry we could not search the store for annotations";
        } else if (action === 'read') {
            message = "Sorry we could not " + action + " the annotations from the store";
        }
        switch (jqXHR.status) {
            case 401:
                message = "Sorry you are not allowed to " + action + " this annotation";
                break;
            case 404:
                message = "Sorry we could not connect to the annotations store";
                break;
            case 500:
                message = "Sorry something went wrong with the annotation store";
        }
        this.showNotification(message, 'error');
        return console.error("API request failed: '" + jqXHR.status + "'");
    };


    VanillaREST.prototype.showNotification = function (message, type) {
        // TODO prettier notification message
        // TODO fire event onShowNotification so that translator plugin can take care of translating the message first.
        window.alert(message);
        console.log(message);
    };



    return VanillaREST;
}());