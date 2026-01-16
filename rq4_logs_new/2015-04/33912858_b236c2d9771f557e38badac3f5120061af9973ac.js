$(document).ready(function(){

  // When an assign ta button is clicked fire off some AJAX
  $('.assign_ta_button').click(function() {
  	 // Get the button that was clicked
  	 var $clicked_button = $(this);
  	 var class_id = $clicked_button.attr('id');
  	 // Disable the button afer it's been clicked
  	 $clicked_button.prop('disabled', true);
    $.ajax({
        type: "POST",
        url: "./../../controller/administrator/get_applied_ta_list.php/",
        data: {
            classID: class_id
        },
        // On success, load a form that allows for TA assignment
        success: function(html) {
            $clicked_button.after(html);
        }   
    });    
    return false;    
  });
});