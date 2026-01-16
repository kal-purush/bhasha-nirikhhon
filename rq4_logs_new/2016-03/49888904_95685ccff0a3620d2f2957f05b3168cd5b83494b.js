/**
 * Created by user on 08/03/2016.
 */

$(document).ready(function(){

   $("#rech_domaine").change(function(){

      var domaine = $(this).val();
       var themes = $("#rech_theme");

       themes.html('<option value="">---Theme---</option>');

       $.post("/theWood/back/Controllers/C_loadTheme.php",{
           "idDomaine" : domaine
       })
       .done(function(data){

           json = JSON.parse(data);
           $.each(json, function(key, val){
               themes.append('<option value="' + val.idTheme + '">' + val.libelleTheme + '</option>');
           })

       })

   });


});