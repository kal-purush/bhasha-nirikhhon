/// <reference path="typings/jquery/jquery.d.ts" />
///<reference path="typings/jquery.color/jquery.color.d.ts"/>

$('*').each(function () {
    if ($(this).css('background-color') != 'rgba(0, 0, 0, 0)') {
        var elementColor = jQuery.Color($(this), 'background-color');
        var elementNewColor = jQuery.Color(300 - elementColor.red(), 300 - elementColor.green(), 300 - elementColor.blue(), elementColor.alpha());
        console.log($(this), elementColor, elementNewColor);
        $(this).css('background-color', elementNewColor);
    }
});
