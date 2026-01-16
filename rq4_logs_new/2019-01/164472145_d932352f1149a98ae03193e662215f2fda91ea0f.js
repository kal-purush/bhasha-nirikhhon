/*****************

Title of Project
Author Name

This is a template. You must fill in the title,
author, and this description to match your project!

******************/
"use strict";

$(document).ready(setup);
  // This code will run when the document is ready
let secretsFound = 0;
let allSecrets = 6;

function setup () {
  setInterval(update,500);
  $('span.redacted').on('click',spanClicked);
  $('span.secret').on('mouseover',spanHovered);
}

function update() {
  $('span.redacted').each(updateSpan)
}

function updateSpan() {
  let num = Math.random();
  if(num <= .1) {
    $(this).removeClass("redacted");
    $(this).addClass("revealed");
  }
}

function spanClicked() {
  $(this).removeClass("revealed");
  $(this).addClass("redacted");
}

function spanHovered() {
  $(this).addClass("found");
  $(this).off('mouseover');
  secretsFound += 1;
  $( "span#secret-count" ).text(secretsFound);
  if (secretsFound === allSecrets) {
      $('span.found').fadeTo(2000, 0.2).fadeTo(2000, 1);
  }
}