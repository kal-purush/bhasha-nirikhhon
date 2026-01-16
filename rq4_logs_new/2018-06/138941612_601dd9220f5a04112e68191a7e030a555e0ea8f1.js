import $ from "jquery";
import axios from "axios";

$(function(){
    const totalPages = $("#totalPagesId").val();
    $(document).ready(function(){
        $('#pagination-demo').twbsPagination({
            totalPages: totalPages,
            visiblePages: 10,
            onPageClick: function (event, num) {

                axios.get('/api/customers',
                    {
                        params: {
                            page: num,
                            size: 10
                        }
                    })
                    .then(function (response) {
                        //if response not empty

                        let ht = "";

                        response.data.map(customer => {
                            ht += "" +
                                "<tr>" +
                                "<td>"+customer.customer_id+"</td>" +
                                "<td>"+customer.store_id+"</td>" +
                                "<td>"+customer.first_name+"</td>" +
                                "<td>"+customer.last_name+"</td>" +
                                "<td>"+customer.email+"</td>" +
                                "<td>"+customer.address_id+"</td>" +
                                "<td>"+customer.active+"</td>" +
                                "<td>"+customer.create_date+"</td>" +
                                "<td>"+customer.last_update+"</td>";
                        });
                        $('#page-content').html(ht);
                    })
                    .catch(function (error) {
                        console.log(error);
                    });
            }
        });
    });
});