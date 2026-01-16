import LayerModule = require("./Layer");
import EntityModule = require("./Entity");

module Canvas {
    export import Entity = EntityModule.Entity;
    export import Layer = LayerModule.Layer;
}

export = Canvas;