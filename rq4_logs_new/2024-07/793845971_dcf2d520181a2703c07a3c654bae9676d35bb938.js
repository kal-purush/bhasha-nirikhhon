#!/usr/bin/node

$('document').ready(function () {
  $.getJSON('https://hellosalut.stefanbohacek.dev/?lang=fr', function (data) {
    $('#hello').text(`${data.hello}`);
  });
});