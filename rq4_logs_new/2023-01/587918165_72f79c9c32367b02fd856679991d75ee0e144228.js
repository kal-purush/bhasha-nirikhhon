  // Instalar Plugin 'chrome plugin advanced rest client'
  // Probar el servicio
  // https://www.themealdb.com/api/json/v1/1/categories.php
  // Llamar el servicio y mostrar los elementos en la tabla categoria


$(document).ready(function () {

    // [jqclick]
     $("#enviar").click(function()
     {
         $.get("https://www.themealdb.com/api/json/v1/1/categories.php",
         function(data)
         {

          $.each(data.categories, function(i, item){
            console.log(data.categories);
                  $("#categorias").append("<tr><td>"+item.idCategory+"</td><td>"+item.strCategory+
                  "</td><td><img src='"+item.strCategoryThumb+"'>"+
                  "</td><td>"+item.strCategoryDescription+"</td></tr>");
              });
            
            
         })
     })
    
    // // [jqajax]
    // $.ajax({
    //     type: "GET",
    //     url: "https://www.themealdb.com/api/json/v1/1/categories.php",
    //     dataType: "json",
    //     success: function (data) {
    //         $.each(data.categories, function(i, item){
    //             $("#categorias").append("<tr><td>"+item.idCategory+"</td><td>"+item.strCategory+
    //              "</td><td><img src='"+item.strCategoryThumb+"'>"+
    //              "</td><td>"+item.strCategoryDescription+"</td></tr>");
    //             });
    //         }
    //     });
})


// $.each(data.categories, function(i, item){
            //      $("#categorias").append("<tr><td>"+item.idCategory+"</td><td>"+item.strCategory+
            //      "</td><td><img src='"+item.strCategoryThumb+"'>"+
            //      "</td><td>"+item.strCategoryDescription+"</td></tr>");
            //  });