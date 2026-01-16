{
    init: function(elevators, floors) {
        var DOWN = 0;
        var UP = 1;

        var floorWaiting = [];

        $.each(floors, function(index, floor) {
            floorWaiting[index] = [];
            floorWaiting[index][DOWN] = false;
            floorWaiting[index][UP] = false;
            floor.on("up_button_pressed", function() {
                floorWaiting[floor.floorNum()][UP] = true;
            });
            floor.on("down_button_pressed", function() {
                floorWaiting[floor.floorNum()][DOWN] = true;
            });
        });

        $.each(elevators, function(index, elevator) {
            var buttonsPressed = [];
            elevator.on("idle", function() {
                //places to go?
                if(buttonsPressed.length > 0) {
                    //filter out double destinations
                    var unique = [];
                    $.each(buttonsPressed, function(index, button) {
                        if($.inArray(button, unique) === -1) {
                            unique.push(button);
                        }
                    });
                    buttonsPressed = unique;
                    elevator.goToFloor(buttonsPressed.splice(0,1)[0]);
                }
                //pick up waiting
                else {
                    var targetFloor = 0;
                    var minDistance = 100000;
                    $.each(floorWaiting, function(index, floor) {
                        if(floor[UP] || floor[DOWN]) {
                            if(Math.abs(index - elevator.currentFloor()) < minDistance) {
                                targetFloor = index;
                                minDistance = Math.abs(index - elevator.currentFloor());
                            }
                        }                        
                    });
                    elevator.goToFloor(targetFloor);                    
                }
            });
            elevator.on("stopped_at_floor", function (floor) {
                floorWaiting[floor][UP] = false;
                floorWaiting[floor][DOWN] = false;
            });
            elevator.on("floor_button_pressed", function (floor) {
                buttonsPressed.push(floor);
            });
        }); 
    },
        update: function(dt, elevators, floors) {
            // We normally don't need to do anything here       
        }
}