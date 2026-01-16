$(document).ready(function(){
  $("#click").click(function(){
    $("#load").text('<?php include_once "include.php";?>');
  });
});