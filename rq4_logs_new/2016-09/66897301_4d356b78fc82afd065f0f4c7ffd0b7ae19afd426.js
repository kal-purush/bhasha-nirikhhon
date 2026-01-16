var detailsArr=[
    "We will compete in an off season FRC competition with the robot we have been working on. The event starts off with practice matches, then we will go into qualification matches, elimination matches, and hopefully finals! If you're not in a team we still invite you to come out and watch to support our team! Please email <a href=\"mailto:engineeing@oastem.org\">engineering@oastem.org</a> for more information.",
    "We will be working on packing our tools for the competition as well as practicing driving our robot around.",
    "We will be working on pur robot for the Fall Classic competiton. The robot arm will be mounted and tested with the mentors. This is also the last day to build.",
    "We will be working on pur robot for the Fall Classic competiton. The robot arm will be mounted and tested with the Mr. Peralta.",
    "We will be working on pur robot for the Fall Classic competiton. The robot arm will be mounted and tested with the Mr. Peralta.",
    "We will be working on pur robot for the Fall Classic competiton. The robot arm will be fixed and refurbished for use.",
    "Come check out our booth during club rush  and sign up for STEM! It will be a blast, we promise!"
];

var locationArr=[
    "Valencia High School",
    "Room 902",
    "Room 902",
    "Room 902",
    "Room 902",
    "Quad"
];

var timeArr=[
    "8am - 5pm",
    "3 - 4:30pm",
    "9am - 12pm",
    "3 - 5pm",
    "3 - 4:30pm",
    "Sept 12 - 15, Lunch"
];

function clearSelected(){
    $(".event-container").each(function(){
        $(this).removeClass("selected");
        $(this).css("background-color","white");
    });
}

function toggleEvtInfo(){
    $(".evt-info-wrap").animate(
        {
            width: 'toggle'
        },
        500
    );
}

$(document).ready(function(){
    $(".event-container").on("click",function(){    
    
        clearSelected();
        $(this).find(".evt-summary").addClass("selected");

        var color = $(this).find(".date-box").css("background-color");
        
        $("#evt-info").css("border-left","3em " + color + " solid");
        $("#evt-info-close").css("color",color);

        var title = $(this).find(".evt-summary h3").text();
        $("#evt-title").text(title);

        var eid = $(this).data("eid");
        $("#evt-loc").text(locationArr[eid]);
        $("#evt-time").text(timeArr[eid]);
        $("#evt-desc").html(detailsArr[eid]);

        if(screen.width < 600) toggleEvtInfo();
    });
});