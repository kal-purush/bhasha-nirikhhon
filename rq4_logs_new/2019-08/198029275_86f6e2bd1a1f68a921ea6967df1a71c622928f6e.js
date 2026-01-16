$(document).ready(function()
{
    $('.row').each(function()
        {
            $(this).children(".text").height($(this).find("img").css("height"));
        });
});