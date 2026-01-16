/**
 * JavaScript file for the Categories view functionality
 */

var gridViewBtnId = "gridViewBtn";
var gridContainerId = "categories-grid";
var listViewBtnId = "listViewBtn";
var listContainerId = "categories-list";

$(document).ready(function() {
  // set up grid and list views
  setUpGridAndListView("#" + gridViewBtnId, "#" + listViewBtnId,
    "#" + gridContainerId, "#" + listContainerId);

});