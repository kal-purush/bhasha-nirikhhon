var workerid = {{worker.id}}
$(function(){
      $("#btn").click(function(){
            $.ajax({
            url: "/updateEmployee",
            type: 'POST',
            data: {
            marriage: $("input[name=optionsRadios]:checked").val(),
            gender: $("input[name=optionsRadios2]:checked").val(),
            type: $("input[name=optionsRadios3]:checked").val(),
            eDate: $("#datepicker").val(),
            probation: $("input[name=optionsPro]:checked").val(),
            pd: $("#pd").val(),
            pw: $("#pw").val(),
            pm: $("#pm").val(),
            idnum: $("#idnum").val(),
            chinese: $("#chinese").val(),
            english: $("#english").val(),
            address: $("#address").val(),
            home: $("#home").val(),
            mobile: $("#mobile").val(),
            salary: $("#salary").val(),
            title: $("#title").val(),
            remarks: $("#remarks").val(),
            workerid: var workerid
            },
            success: function(){
            alert("updated");
          },
            });
        });
    });