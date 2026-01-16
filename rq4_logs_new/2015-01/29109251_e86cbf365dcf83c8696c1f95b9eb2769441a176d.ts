/// <reference path="../../types/react.d.ts" />

/**
 * Created by Stephan on 12.01.2015.
 */

"use strict";

import React = require("react");

import Actions = require("fluss/baseActions");


export var Button = React.createClass({

   handleClick: function() {
      Actions.undo();
   },

   render: function() {
      return React.DOM.button({ id: "undo", onClick: this.handleClick }, "Undo");
   }

});