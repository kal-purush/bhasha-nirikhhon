/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

$(document).ready(function() {
    $("#resource").change(function() {
        fillTable($(this).val());
    });
    
    fillTable($("#resource").val());
    
    function fillTable(resourceid) {
        $.ajax({
            url: 'table.php',
            type: 'GET',
            data: { resourceid: resourceid },
            success: function(data, status) {
                $("#table_content").html(data);
            }
        });
    }
});