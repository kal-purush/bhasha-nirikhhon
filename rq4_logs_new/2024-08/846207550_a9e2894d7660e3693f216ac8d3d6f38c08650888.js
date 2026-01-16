jQuery(document).ready(function ($) {
  var isChecked = false;
  $("#switch_vehicle").prop("checked", isChecked);

  if (isChecked) {
    $("#form-vehicle").css({
      visibility: "hidden",
      position: "absolute",
    });
    $("#form-dimension").css({
      visibility: "visible",
      position: "relative",
    });
  } else {
    $("#form-vehicle").css({
      visibility: "visible",
      position: "relative",
    });
    $("#form-dimension").css({
      visibility: "hidden",
      position: "absolute",
    });
  }

  // Lógica cuando se cambia el valor del switch
  $("#switch_vehicle").change(function () {
    if ($(this).is(":checked")) {
      $("#form-vehicle").css({
        visibility: "hidden",
        position: "absolute",
      });
      $("#form-dimension").css({
        visibility: "visible",
        position: "relative",
      });
    } else {
      $("#form-vehicle").css({
        visibility: "visible",
        position: "relative",
      });
      $("#form-dimension").css({
        visibility: "hidden",
        position: "absolute",
      });
    }
  });

  function initializeSelect2(
    selector,
    option,
    placeholder,
    dependentSelectId = null
  ) {
    $(selector)
      .select2({
        ajax: {
          url: tiresFormData.ajax_url,
          dataType: "json",
          delay: 250,
          data: function (params) {
            let requestData = {
              action: "tires_form_search",
              option,
              term: params.term,
            };

            if (dependentSelectId) {
              const terms = dependentSelectId.split(',');
              terms.forEach(element => {
                requestData[element] = $("#" + element).val();
              });
            }

            return requestData;
          },
          processResults: function (data) {
            return {
              results: data,
            };
          },
          cache: true,
        },
        placeholder: placeholder,
        language: {
          inputTooShort: function (args) {
            return "Por favor ingresar " + args.minimum + " o más caracteres";
          },
          searching: function () {
            return "Buscando...";
          },
          noResults: function () {
            return "No se encontraron resultados";
          },
        },
        open: function () {
          var select = $(this);
          if (!select.data("loaded")) {
            select.data("loaded", true);
            select.select2("open");
          }
        },
        width: "resolve",
      })
      .css("min-width", "150px");
  }

  initializeSelect2("#mark", "mark_search", "Marca");
  initializeSelect2("#year", "year_search", "Año", "mark");
  initializeSelect2("#line", "line_search", "Línea", "mark,year");
  initializeSelect2("#version", "version_search", "Versión", "mark,year,line");
  initializeSelect2("#width", "width_search", "Ancho");
  initializeSelect2("#ratio", "ratio_search", "Perfil");
  initializeSelect2("#rim", "rim_search", "Rin");
});