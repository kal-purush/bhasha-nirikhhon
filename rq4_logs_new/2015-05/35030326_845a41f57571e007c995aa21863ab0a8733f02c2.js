/*
 * @fileOverview
 * @author: Mike Grabowski (@grabbou)
 */

'use strict';

var Chirpy = require('../lib');

var myProject = new Chirpy({
  token: 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjU1NGNhYmE2NDc0Mjg5NGViYTZmMjhkMiIsImlhdCI6MTQzMTQ2Njc2NH0.1LPC9xrDhoiF_z33Usy5UDPNCRzKqDH8NXbfNSVVYwg',
  host: 'localhost',
  port: 3000
});

myProject.track('hey!', function(err) {
  console.log(err);
});