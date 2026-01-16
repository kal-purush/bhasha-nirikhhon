// todo list
//   three difficulty modes
//      hard: start over; speed faster
//  startup sequence ?
//  add fancy font for digital display

var simon = {
    seq: [],
    user:  [],
    mode:  undefined,
    turn: undefined,
    round: 0,
    on: false,
    difficulty: 500, //  change to adjust difficulty, must be above 250 due to highlight duration


    //starts the machine and binds the start button
    //maybe add a startup sequence here? 
    init: function() {
        var slide = document.querySelector('.slide');
        var start = document.querySelector('.start');
        var mode = document.querySelector('.mode');
        slide.addEventListener('click', function() {
            simon.onOff(); 
            });
        start.addEventListener('click', function() {
            simon.start();
        });

        mode.addEventListener('click', function() {
            simon.modeChange();
        });
        
        //var mode = document.querySelector('.mode'); // difficulty toggle
    },

    activate: function() {
        if(!simon.on) {
            simon.stop()
        } 
    },
    
    //cosmetic on/off button plus triggers the AI to go 
    onOff: function(){
        if(!simon.on) {
            document.querySelector('.inside').className = "inside on";
            simon.on = true;
            document.querySelector('.round').innerHTML = '--';
        } else {
            document.querySelector('.inside').className = "inside";
            simon.on = false;
        }
        simon.activate();
    },

    // starts the game
    start: function() {
        //reset our variables
        this.seq = [];
        this.user = [];
        this.round = 0;
        this.turn = 'ai';
        simon.roundAdjust();
        simon.goAI();   
    }, 
    
    stop: function(){
        this.seq = [];
        this.round = 0;
        this.user = [];
        document.querySelector('.round').innerHTML = '';
        this.turn = '';
    },

    modeChange: function() {
        if(simon.mode === undefined) {
            simon.mode = 'strict';
            document.querySelector('.mode').className = 'btn mode lit';
        } else {
            simon.mode = undefined;
            document.querySelector('.mode').className = 'btn mode';
        }
        console.log(simon.mode);
    },

    beep: function(x) {
        var meep = new Audio();
        var sound = ['https://s3.amazonaws.com/freecodecamp/simonSound1.mp3',
                       'https://s3.amazonaws.com/freecodecamp/simonSound2.mp3',
                       'https://s3.amazonaws.com/freecodecamp/simonSound3.mp3', 
                       'https://s3.amazonaws.com/freecodecamp/simonSound4.mp3' ]
        meep.src = sound[x];
        meep.play();
    },

    //highlight a color + play a sound
    selectColor: function(color, noSound){
        var ele = document.querySelector('.color[data-color="' + color +'"]');
        console.log('color#: ' + ele.id);
 
        if (noSound === undefined) {
            simon.beep(color);
        }

        //highlight
        document.getElementById(ele.id).className = "color pushed";
        setTimeout( function() {
            document.getElementById(ele.id).className = "color";
        }, ( 250 ) ); 
    }, //end selectColor

    // ai turn 
    goAI: function(){
        //check for the win condition and advances turn if not found
        checkWin();       
        
        // looping through array function
        function seqLoop() {
            var loops = simon.seq.length;
            console.log(' seqLoop has fired. length: ' + loops)

            //..the loop
            var i = 0;
            var theLoop = setInterval(function() {
                var color = simon.seq[i];
                //console.log('firing selectColor#' + i)
                simon.selectColor(color);
                i++;

                if(i >= loops) {
                    console.log('loop cleared')
                    window.clearInterval(theLoop);
                    //advances turn if it is the ai's turn
                    if(simon.turn === 'ai') {
                        setTimeout( function() {
                            addOne();
                        }, simon.difficulty)
                    }
                }    
            }, simon.difficulty ); 
        } //end seqLoop
        
        //ai's new move
        function addOne(){
            var num = Math.floor(Math.random() * 4);
            simon.seq.push(num);
            simon.selectColor(num);
            setTimeout( function() {
                simon.goUser();
            }, 500);   
        }

        function checkWin() {
            if (simon.round === 20) {
                victory();
            } else {
                nextTurn();
            }
        }

        function nextTurn() {
            if(simon.turn === 'ai') {
                simon.round++;
                simon.roundAdjust();
            }
            simon.user = [];

            if(simon.seq.length === 0){
                addOne();
            } else {
                seqLoop();
            }
        }

        function victory() {
            //alert('nice work kid');
            all();

            setTimeout( function() {
                all(); 
            }, 500 )

            setTimeout( function() {
                simon.onOff();
                alert('Congratulations!! You have won the game. Press start to play again');
            }, 1000 )
            
            function all(){
                for(var i = 0; i < 4; i++) {
                    simon.selectColor(i, 'y');
                }
            }

            // function again() {
            //     var reply = prompt('Would you like to play again? ("yes" or "no")').toLowerCase();
            //     console.log(reply);
            //     if (reply === 'yes') {
            //         simon.start();
            //     }

            //     if(reply === 'no') {
            //         alert('thanks for playing!');
            //     }
            //     else {
            //         alert('hmmm, your fingers must have slipped');
            //         again();
            //     }
            // }
        }

    }, //end go AI

    //user function
    goUser: function(){
        // goUser variables
        console.log('_______________user turn_________________')
        console.log('AIlog: ' + simon.seq + ' player log: ' + simon.user);
        var cButs = document.querySelectorAll('.color');
        
        // goUser main logic
        simon.turn = 'user';
        enable();

        // helper functions for goUser
        function check() {
            var arr1 = simon.seq;
            var arr2 = simon.user;
            var counter = 0;

            arr2.forEach((e, i, arr) => {
                if(arr1[i] == e) {
                    console.log('you cho-cho-chose wisely for #' + i); 
                    counter++; 
                }
                if(arr1[i] != e) {
                    console.log('you dun messed up for #' + i + '; try again');
                    document.querySelector('.round').innerHTML = '!!';
                    simon.user = [];
                    counter = 0;

                    if(simon.mode === 'strict') {
                        simon.start();
                    } else {
                        setTimeout( function() {
                            simon.goAI();
                        }, 500);
                    }

                    
                }
            });

            //advances the turn if the arrays are the right size and match fully
            if(counter === arr1.length ) {
                disable();
                simon.turn = 'ai';
                setTimeout( function() {
                        simon.goAI()
                    }, 500);  
            }
        };

        function enable() {
            cButs.forEach(e => {
                if(e.dataset.listener === 'false') {
                    e.addEventListener('click', bind)
                    e.dataset.listener = 'true';
                }
            });
        };

        function disable() {
            cButs.forEach(e => {
                e.removeEventListener('click', bind);
                e.dataset.listener = 'false';
            });
        };

        function bind() {
            if( simon.turn === 'user') {
                console.log(' I clicked ' + this.id + ' ' + this.dataset.color);
                simon.user.push(this.dataset.color);
                simon.selectColor(this.dataset.color); //passes 0-3, and the array to log the value.   
                check();  
            }
        };
    
    }, //end goUser

    //displays the round
    roundAdjust: function() {
        var adj = adjust(simon.round);
        document.querySelector('.round').innerHTML = adj;
        function adjust(n){
            return (n < 10) ? ("0" + n) : n;
        }
    }

} //end simon

window.onload = simon.init();