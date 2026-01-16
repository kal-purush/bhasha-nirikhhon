$(document).ready(function() {
    // We are going to set a timeout to mock like we are hitting the database with a lot of tables.
    setTimeout(function() {
    $.ajax({
        url: "/clients.php",
        type: "POST",
        data: {
            'action':'getAllClients'
        },
        dataType: "json",
        success: function(data) {
            var tableData = '<table class="table table-hover"><tr><th>Client Id</th><th>Client Name</th><th>Company Name</th></tr>';
            data.forEach(function(e) {
                tableData = tableData + '<tr><td>'+e.id+'</td><td>'+e.name+'</td><td>'+e.company+'</td></tr>';
            })
            tableData = tableData + '</table>';

            $('.clientTable').html(tableData);
            $('.fetchData').hide();
        }
    });}, 5000);
});