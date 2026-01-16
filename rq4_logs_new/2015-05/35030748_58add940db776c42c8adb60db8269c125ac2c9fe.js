//GameInterface Class
var GameInterface = function (properties) {
    this.$header;
    this.$body;
    this.$footer;
    this.$map;
    this.properties = properties;
    this.Initialize();
};

GameInterface.prototype.Initialize = function () {
    this.Header();
    this.Body();
    this.Footer();
};

GameInterface.prototype.Header = function () {
    this.$header = $('<div/>', {
        id: 'gameheader',
        class: 'small-12'
    }).appendTo($('#gamecontainer'));

    for (var ele in this.properties.header) {
        switch (ele) {
            case('background'):
                if (this.properties.header[ele]) {
                    this.Header.setBackgroundColor(this, this.properties.header[ele]);
                }
                break;
            case('height'):
                if (this.properties.header[ele]) {
                    this.Header.setHeight(this, this.properties.header[ele]);
                }
                break;
            case('elements'):
                var elements = this.properties.header[ele];
                for (var element in elements) {
                    this.Header.addButton(this, elements[element]);
                }
                break;
        }
    }
};

GameInterface.prototype.Body = function () {
    this.$body = $('<div/>', {
        id: 'gamebody',
        class: 'small-12'
    }).appendTo($('#gamecontainer'));
    
    for (var ele in this.properties.body) {
        switch (ele) {
            case('background'):
                if (this.properties.body[ele]) {
                    this.Body.setBackgroundColor(this, this.properties.body[ele]);
                }
                break;
            case('height'):
                if (this.properties.body[ele]) {
                    this.Body.setHeight(this, this.properties.body[ele]);
                }
                break;
            case('map'):
                if (this.properties.body[ele]) {
                    this.Body.addMap(this);
                }
                break;
        }
    }
};

GameInterface.prototype.Footer = function () {
    this.$footer = $('<div/>', {
        id: 'gamefooter',
        class: 'small-12'
    }).appendTo($('#gamecontainer'));

    for (var ele in this.properties.footer) {
        switch (ele) {
            case('background'):
                if (this.properties.footer[ele]) {
                    this.Footer.setBackgroundColor(this, this.properties.footer[ele]);
                }
                break;
            case('height'):
                if (this.properties.footer[ele]) {
                    this.Footer.setHeight(this, this.properties.footer[ele]);
                }
                break;
            case('elements'):
                var elements = this.properties.footer[ele];
                for (var element in elements) {
                    this.Footer.addButton(this, elements[element]);
                }
                break;
        }
    }
};

GameInterface.prototype.Header.setBackgroundColor = function (GI, color) {
    GI.$header.css('background-color', color);
};

GameInterface.prototype.Header.setHeight = function (GI, height) {
    GI.$header.css('height', GI.convertpCentToPixel(height));
};

GameInterface.prototype.Body.setBackgroundColor = function (GI, color) {
    GI.$body.css('background-color', color);
};

GameInterface.prototype.Body.setHeight = function (GI, height) {
    GI.$body.css('height', GI.convertpCentToPixel(height));
};

GameInterface.prototype.Body.addMap = function (GI) {
    $map = GI._Map();
    $map.appendTo(GI.$body);
    GI._Map.initialize(GI);
};

GameInterface.prototype.Footer.setBackgroundColor = function (GI, color) {
    GI.$footer.css('background-color', color);
};

GameInterface.prototype.Footer.setHeight = function (GI, height) {
    GI.$footer.css('height', GI.convertpCentToPixel(height));
};

GameInterface.prototype.Header.addButton = function (GI, properties) {
    $button = GI._Button(properties);
    $button.appendTo(GI.$header);
};

GameInterface.prototype.Footer.addButton = function (GI, properties) {
    $button = GI._Button(properties);
    $button.appendTo(GI.$footer);
};

GameInterface.prototype._Button = function (properties) {
    var _class = 'button';
    if (properties.align) {
        _class += ' ' + properties.align;
    }

    return $('<a/>', {
        'class': _class,
        'id': properties.id,
        'text': properties.text
    });
};

GameInterface.prototype._Label = function () {

};

GameInterface.prototype._TextField = function () {

};

GameInterface.prototype._TextArea = function () {

};

GameInterface.prototype._Map = function () {
    return $('<div/>', {
        'class': 'map',
        'id': 'map',
        'style': 'height:100%;width:100%;'
    });
};

GameInterface.prototype._Map.initialize = function (GI) {
    var styles = [
        'Road',
        'Aerial',
        'AerialWithLabels',
        'collinsBart',
        'ordnanceSurvey'
      ];
    GI.$map = new ol.Map({
        target: 'map',
        layers: [
            new ol.layer.Tile({
                source: new ol.source.BingMaps({
                    key: 'Ak-dzM4wZjSqTlzveKz5u0d4IQ4bRzVI309GxmkgSVr1ewS6iPSrOvOKhA-CJlm3',
                    imagerySet: styles[2]})
            })
        ],
        view: new ol.View({
            center: ol.proj.transform([7.623411, 51.959184], 'EPSG:4326', 'EPSG:3857'),
            zoom: 13
        })
    });
};

GameInterface.prototype.convertpCentToPixel = function (pCent) {
    var Pixel = $(window).height() / 100 * pCent;
    return Pixel;
};
