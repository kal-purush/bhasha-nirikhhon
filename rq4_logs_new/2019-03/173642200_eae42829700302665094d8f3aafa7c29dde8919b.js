var content = '<div class="home-section home-box big-box">\
<h2 class="home-title">Things I use for...</h2>\
<div class="home-tools">\
    <div id="tools-trigger-dev" data-target="#tools-dev" class="home-tool" style="background: #f66a75;">Development</div>\
    <div id="tools-trigger-design" data-target="#tools-design" class="home-tool" style="background: #d659ae;">Design</div>\
    <div id="tools-trigger-travel" data-target="#tools-travel" class="home-tool" style="background: #b266e3;">Travel</div>\
    <div id="tools-trigger-remote" data-target="#tools-remote" class="home-tool" style="background: #795dea;">Remote work</div>\
</div> \
<div class="tools-wrap">\
    <div id="tools-dev" class="home-tools-imgs">\
        <img alt="Javascript" src="/assets/js.png">\
        <img alt="jQuery" src="/assets/jquery.png">\
        <img alt="CSS" src="/assets/css.png">\
        <img alt="SASS" src="/assets/sass.png">\
        <img alt="PHP" src="/assets/php.png">\
        <img alt="Grunt" src="/assets/grunt.png">\
        <img alt="VSCode" src="/assets/vscode.png">\
        <img alt="Git" src="/assets/git.png">\
    </div>\
    <div id="tools-design" class="home-tools-imgs">\
        <img alt="Photoshop" src="/assets/ps.png">\
        <img alt="Illustrator" src="/assets/illustrator.jpeg">\
        <img alt="After Effects" src="/assets/ae.png">\
    </div>\
</div>\
</div>';

$(function() {
    var allDivs = $("#tools-dev, #tools-design, #tools-travel, #tools-remote");
    var allTriggers = $("#tools-trigger-dev, #tools-trigger-design, #tools-trigger-travel, #tools-trigger-remote");
    allDivs.hide();
    $("#tools-dev").show();

    allTriggers.click(function(){
        allDivs.fadeOut();
        el = $(this).attr("data-target");
        setTimeout( function() { 
            $(el).fadeIn('fast');
        }, 500);
    });
});