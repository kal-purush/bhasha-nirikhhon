/*
* @Author: Daniel Goberitz
* @Date:   2017-02-18 22:24:22
* @Last Modified by:   danyg
* @Last Modified time: 2017-02-18 22:26:58
*/

define([
	'jquery',
	'text!./toolbar.html',
	'css!./toolbar.css'
], function(
	$,
	toolbarHTML
) {

	'use strict';

	$(() => {
		var container = $(toolbarHTML);
		$('#toolbar').replaceWith(container);
	});

});