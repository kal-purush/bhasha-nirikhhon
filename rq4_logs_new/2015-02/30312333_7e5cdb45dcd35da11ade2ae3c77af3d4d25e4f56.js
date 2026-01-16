$(document).ready(function(){
  $('ul.clinic_list > li').hide();
  $('li.section_1').show();

  $('ul.link_list > li > a').click(function(){
    $('ul.clinic_list > li').hide();
    var clinic_list = $(this).attr('clinic_num')
    $('li.section_' + clinic_list).show();
  })
})

  $('ul.patient_list > li').hide();
  $('li.section_1').show();