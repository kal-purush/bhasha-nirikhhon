function getUrlParameter(sParam)
{
    var sPageURL = window.location.search.substring(1);
    var sURLVariables = sPageURL.split('&');
    for (var i = 0; i < sURLVariables.length; i++)
    {
        var sParameterName = sURLVariables[i].split('=');
        if (sParameterName[0] == sParam)
        {
            return sParameterName[1];
        }
    }
}

$(document).ready(function() { 
    var invitees = getUrlParameter('invitees');
    console.log("Invitees is", invitees);
    if (invitees) { 
        invitees = invitees.split(";");
        var inviteeString = "";
        for (var i = 0; i < invitees.length; i++) { 
            inviteeString += invitees[i];
            if (i < invitees.length - 1) { 
                console.log("Adding an AND");
                inviteeString += " and ";   
            }
        }
        console.log("Changing invitees to " + inviteeString);
        $("#invitees").text(inviteeString);
    }

//    var formURL = 'https://docs.google.com/forms/d/1VSPxvWxQgJHWibFbpfUhID2-em9dxIjzjmRAdccdXjM/viewform?edit_requested=true&entry_2096183350=booo';
    //var formURL = 'http://maximilianwittmann.com/googleform?edit_requested=true&entry_2096183350=booo&entry.2090035683=true';
    var formURL = "http://maximilianwittmann.com/googleform/submit?entry.2090035683=I'm+coming!+(1+person)&entry.2096183350=john";
    
    $.ajax({
      type: "POST",
      url: formURL,
//      data: data,
      success: function() { 
        console.log("Posted successfully");
      }
//        ,
//      dataType: dataType
    });

    $.post( "ajax/test.html", function( data ) {
        $( ".result" ).html( data );
    });
    
//    $('.ss-top-of-page').hide();
//    $('.ss-footer').hide();
});