/* 
most jquery js documents start
with the $(document).ready function.
This ensures the document is
ready for the code inside of the
function to be executed 
*/

$(document).ready(function() {
    
    /* 
    jquery follows its own syntax. it uses
    the $ symbol to look for an element. 
    followed by () which is the element to find
    inside of the parentheses. followed a
    . and an event. this event then triggers the
    following function, executing the code
    inside the curly brackets.
    */

    $('body').click(function(){
        $(this).css('background', 'yellow');
    })

    /* random background color from an array */

    // let colors = ['blue', 'red', 'green', 'purple', 'yellow']
    // $('body').click(function(){
    //     var color = colors[Math.floor(Math.random() * colors.length)];
    //     $(this).css('background', color);
    // });

    /* add a character to a list */

    // $('li').mouseover(function(){
    //     $(this).append('👋');
    // });

    /* make a bug follow your cursor and resize */

    // $('body').append('<div id="follow">🦟</div>');
    // $(document).on("mousemove", function (event) {
    //     $('#follow').css({
    //         'font-size' : event.pageY/10+'px',
    //         'position' : 'absolute',
    //         'left' : event.pageX+10+'px',
    //         'top' : event.pageY+10+'px'
    //     });
    // });


});