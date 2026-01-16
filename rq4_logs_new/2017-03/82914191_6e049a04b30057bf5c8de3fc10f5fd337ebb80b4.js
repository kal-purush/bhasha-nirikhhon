/**
 * Created by PC on 3/3/2017.
 */


function loadViewModalData(data) {
    $("#view").html("");

    $.ajax({
        url: "http://localhost:3000/auctions/edit",
        type: "POST",
        dataType: "json",
        data: data,
        success: function (data, textStatus, jqXHR) {
            var vdt = jqXHR.responseJSON;
            // vdt[0].forEach(updateView1);
            // vdt[2].forEach(updateView2);
            updateView1(vdt);
            vdt[2].forEach(updateView2);
            $("#myView").modal();
        }
    })

}

//{"auction_id":"abcd1234","description":"Fab needed to be imports from China","name":"Warehouse #3 LDA","due_date":"2017-03-23T18:30:00.000Z","start_time":"15:30:00","end_time":"16:30:00","date_created":"2017-02-01T18:30:00.000Z","created_by":"mas_admin"}

function updateView1(data) {

    $('#view').append(

    '<div class="col-lg-12" xmlns="http://www.w3.org/1999/html">'+
    '<div class="panel glyphicon-text-color panel-body"  style="color:#122b40">'+
    '<div class="panel-heading">'+
    '<div class="row">'+
    '<div><h4><strong><center>'+data[0].name+'</center></strong></h4></div>'+
    '</div>'+
    '</div>'+
    '<div class="panel-footer">'+
    '<div><strong>Auction ID          :  '+data[0].auction_id+'</strong></div>'+
    '<div><strong>Auction Due Date    :  '+data[0].due_date+'</strong></div>'+
    '<div><strong>Auction Create Date :  '+data[0].date_created+'</strong></div>'+
    '<div><strong>Auction Start Time  :  '+data[0].start_time+'</strong></div>'+
    '<div><strong>Auction End Time    :  '+data[0].end_time+'</strong></div>'+
    '<div><strong>Auction Description :  '+data[0].description+'</strong></div>'+
    '<br>'+
    '<br>'+
    '</div>'+
    '</div>'+
    '</div>'

    )
}
function updateView2(data) {

    $('#view').append(
        '<div class="col-lg-4">'+
            '<div class="panel panel-primary"  style="color:#0d8aad">'+
                '<div class="panel-heading">'+
                        '<div class="row">'+
                            '<div><strong><center>'+data.item_name+ '</center></strong></div>'+
                        '</div>'+
                '</div>'+
                '<div class="panel-footer">'+
                    '<div><strong>Item ID   : '+ data.item_id + '</strong></div>'+
                '</div>'+
            '</div>'+
        '</div>'

    )
}
var auid;

$(document).on('click', '.view-btn', function (d) {
    auid = this.parentElement.parentElement.firstChild.firstChild.firstChild.childNodes[1].firstChild.innerHTML;
    var object = {"auction_id": auid};

    loadViewModalData(object);
});