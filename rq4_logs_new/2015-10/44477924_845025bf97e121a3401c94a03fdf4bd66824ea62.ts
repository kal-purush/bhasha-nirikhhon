/*
  Project: Arena game
  Author:  Copyright (C) 2015, Atanas Laskov

  License: BSD license, see LICENSE.md for more details.

  http://www.atanaslaskov.com/arena/
*/

/// <reference path="../typings/jasmine/jasmine.d.ts" />
/// <reference path="../src/clock.ts" />

describe("Clock", () => {
  var time = 0.0;
  var timeService = ()=>{ time+=0.5; return time; };

  beforeEach( ()=>{
    time = 0.0;
  });

  it("should be initialized", () => {
    var c = new Clock( timeService );

    expect( c.clock ).toEqual( 0.5 );
    expect( c.fps ).toBeCloseTo( 60.0, 0 );
    expect( c.dt ).toBeCloseTo( 1.0/60.0, 3 );
  });

  it("should measure time", () => {
    var c = new Clock( timeService );

    c.tick();
    expect( c.clock ).toEqual( 1.0 );
  });

  it("should return time delta in seconds", () => {
    var c = new Clock( timeService );
    expect( c.tick() ).toEqual( 0.5 * 0.001 );
  });

  it("should measure frame rate", () => {
    var c = new Clock( timeService );

    c.tick();
    expect( c.fps ).toBeCloseTo( 1.0/(0.5 * 0.001), 0 );
  });
});