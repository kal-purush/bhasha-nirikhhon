$(document).ready(function(){
    $('#category').change(function(){
        var id = $(this).find(":selected").val();
        console.log("ID", id);
        var dataString = 'cat_id='+id;
        $.ajax({
            // url : 'getCategory.php',
            // dataType : 'json',
            // data : dataString,
            // cache : false,
            // success : function(categoryData){
            //     if(categoryData){
            //         console.log("DATA",categoryData);
            //         $('#subcategory').text(categoryData.subcategory);
            //     }
            //     else{
            //         alert("No DATA");
            //     }
            // }
            url : 'addProducts.php',
            data : dataString
        });
    });
});