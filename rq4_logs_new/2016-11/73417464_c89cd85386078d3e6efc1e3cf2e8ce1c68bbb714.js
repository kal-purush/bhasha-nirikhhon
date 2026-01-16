$(document).ready(function () {


  $("#box_movement_cost_center").chosen({
      width: "100%"
  });

  $("#box_movement_box_movement_type").chosen({
      width: "100%"
  });
    $("#box_movement_currency_id").chosen({
      width: "100%"
  });

  $("#box_movement_user_id").chosen({
      width: "100%"
  });

  $('#box_movement_date').datetimepicker({
    language:  'es',
    weekStart: 1,
    autoclose: 1,
    todayHighlight: 1,
    format: "dd MM yyyy - hh:ii"
  });

  $().button('toggle');

  $('.chbx-mon, .chbx-day').on("change", function (e) {
    filterMovement();
  });

  setDefaultMonth();

});


function setDefaultMonth() {
  var d = new Date();
  var actual_month = d.getMonth() +   1 ;
  var chbox = $(".months-group :checkbox[value="+ actual_month +"]")
  chbox.prop("checked","true");
  chbox.parent().addClass("active");
  filterMovement();
}

function filterMovement() {
  months = $(".months-group input:checked").map(function() { return $( this ).val(); }).get().join( "," );
  days = $(".days-group input:checked").map(function() { return $( this ).val(); }).get().join( "," );
  $.ajax({
    method: "GET",
    url: "/box_movements",
    data: { months: months, days: days }
  })  .done(function( data) {
    $('#tab_box_cont').html(data);
    setDataTable();
  });
}

function setDataTable(){
  $('#box_movement_table').dataTable({
    "aoColumnDefs": [{ "bSortable": false, "aTargets": [ 7,8 ] }],
    "bLengthChange": false,
    "bProcessing": true,
    "fnInitComplete": function(oSettings, json) {
      var search_input = (this).closest('.dataTables_wrapper').find('div[id$=_filter] input');
      search_input.attr('placeholder', 'Buscar');
      search_input.addClass('form-control input-small');
      search_input.css('width', '250px');
    },
    "oLanguage": {  "sProcessing": "Procesando...",
                    "sLoadingRecords": "Cargando...",
                    "sLengthMenu": "Mostrar _MENU_ registros",
                    "sZeroRecords": "No se encontraron resultados",
                    "sInfo": "Mostrando desde _START_ hasta _END_ de _TOTAL_ registros",
                    "sInfoEmpty": "No existen registros",
                    "sInfoFiltered": "(filtrado de un total de _MAX_ líneas)",
                    "sInfoPostFix": "",
                    "sSearch": "",
                    "sUrl": "",
                    "oPaginate": { "sFirst":    "Primero",
                                   "sPrevious": "Anterior",
                                   "sNext":     "Siguiente",
                                   "sLast":     "Último" }
    },
    "footerCallback": function ( row, data, start, end, display ) {

    }
  });
}