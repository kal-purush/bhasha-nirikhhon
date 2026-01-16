"use strict";

var eventBus = new EventBus('/eventbus');
var tileWithPeanuts = new TileWithPeanuts();
var peanutSpeed = new PeanutSpeed();
var randomnessStatistics = new RandomnessStatistics();
var piApproximation = new PiApproximation();

window.onload = whenDomIsReady.completed;
eventBus.onopen = whenEventBusIsOpen.completed;

whenEventBusIsOpen
    .thenDo(function() {
      eventBus.registerHandler('peanut.notify', tileWithPeanuts.drawPeanut);
    })
    .thenDo(function() {
      eventBus.registerHandler('peanut.notify', randomnessStatistics.updateDropCounter);
      eventBus.registerHandler('peanut.notify', randomnessStatistics.updateAverageX);
      eventBus.registerHandler('peanut.notify', randomnessStatistics.updateAverageY);
      eventBus.registerHandler('peanut.notify', randomnessStatistics.updateCorrelation);
    })
    .thenDo(function() {
      eventBus.registerHandler('peanut.notify', piApproximation.updatePi);
    });

new CompositeFuture()
    .and(whenSliderIsReady)
    .and(whenEventBusIsOpen)
    .thenDo(function() {
      eventBus.registerHandler('peanut.speed.set', '', peanutSpeed.updatePeanutSpeed);
      eventBus.send('peanut.speed.get', '', peanutSpeed.updatePeanutSpeed);
    });