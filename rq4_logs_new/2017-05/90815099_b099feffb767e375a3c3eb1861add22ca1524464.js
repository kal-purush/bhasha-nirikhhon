/*global $*/

//AUTOSCROLLING FUNCTIONALITY.
//Periodically scrolls down page, then rests, 
//then rewinds back to top once it hits the bottom
//////////////////////////////////////////////////
$(function() {
    var div = $("body");
    var c = 0;                      //Counter that increments every call
    var rate = 60;                  //Framerate of motion (smoothness)
    var interval = 10 * rate;       //Seconds per loop (still / move)
    var ratio = 0.85;                //What % of the time is still?
    var speed = 2;                  //How many pixels per step?
    var rewind = 2;                 //Multiplier for scrolling back up
    var goingDown = true;           //Used to flip directions (don't modify)
    
    //Wait a second for things to load
    setTimeout(function() {
        //Set function every X milliseconds
        setInterval(function() {
            
            //If we're going down
            if (goingDown) {
                //Increment counter % interval
                c = (c + 1) % interval;
                
                //Should we be scrolling down?
                if (c > (interval * ratio)) {
                    
                    //Get current position
                    var pos = div.scrollTop();
                    
                    //Move us down!
                    div.scrollTop(pos + speed);
                }
                
                //If not scrolling, rest
                else {
                    
                    //Get current position
                    var pos = div.scrollTop();
                    
                    //If it's reached the bottom, flip direction
                    if (pos + $(window).height() >= $(document).height()) {
                        
                        //Wait a second at the bottom
                        setTimeout(function() {
                            goingDown = false;
                        }, 2000); //time in MS to wait before rewind
                    }
                }
            }
            //Else we're going up
            else {
                
                //Rewind and check if you've hit the top again.
                div.scrollTop(div.scrollTop() - speed * rewind);
                
                if (div.scrollTop() <= 0) {
                    
                    //Flip tirection;
                    goingDown = true;
                }
            }
        }, 16.6); //setInterval MS is 1000 / Rate
    }, 1000); //timeout function to avoid onLoad errors
    
    
    //! ! ! ! ! ! ! ! ! ! ! 
    //CURRENTLY NOT WORKING
    //Override tweet style
    $( ".timeline-Tweet" ).each(function() {
        $( this ).css( "padding-Top","20px" );
    });
});