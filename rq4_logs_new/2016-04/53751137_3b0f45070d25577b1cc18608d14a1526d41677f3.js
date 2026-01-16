$(document).ready(function (elt) {

    var label = $('label');
    label.each(function (e) {
      if($(this).attr('id')!=='labelIsole'){
       var  divInput = $('<div class="col-lg-9 col-md-9 col-sm-9 col-xs-10"></div>'),
                parentDiv = $(this).parent(), children1 = parentDiv.children('input'),
                children2 = $(this).children('input'),
                children3 = parentDiv.children('select'),
                input1 = children1,
                input2 = children2,
                input3 = children3;
        //alert(input1.attr('class'));
        parentDiv.addClass('row');
        $(this).addClass('col-lg-3 col-md-3 col-sm-3 col-xs-2');
        $(this).after(divInput);
        divInput.prepend(input1);
        divInput.prepend(input2);
        divInput.prepend(input3);
      }
    });
  
   var logo = '<img id="logo" src="/greenice/frontend/web/images/logo.jpg" alt="image_logo"/>';
   $('.navbar-header a').replaceWith(logo);
   $('#logo').css('width','40px').css('heigth','40px').css('margin-top','10px').css('border-radius','4px').css('border','1px solid green');
   $('#w0-collapse ul li a').css('color','white');
});

        