
$(document).ready(function() {
    $(document).on('click', '#delete', function(){
        //console.log('test')
        var id = $(this).attr('data-id')
        delete_user({userid: id})
    })
})

function delete_user(data = {}) {
    Swal.fire({
        title: 'Are you sure?',
        text: "You won't be able to revert this!",
        icon: 'warning',
        showCancelButton: true,
        confirmButtonColor: '#3085d6',
        cancelButtonColor: '#d33',
        confirmButtonText: 'Yes, delete it!'
      }).then((result) => {
        if (result.isConfirmed) {
            $.ajax({
                url: base_url+'dashboard/delete_Employee',
                type: 'post',
                // data: {userid:user_id},
                data: data,
                cache: false,
                async: false,
                success:function(data) {
                    Swal.fire(
                        'Deleted!',
                        'Employee file has been deleted.',
                        'success'
                      ).then(function(){
                        window.location.reload(base_url+'dashboard/index');
                    })
                }
        
            }) 
        }
        else{
            Swal.fire({
                icon: 'info',
                title: 'Yeah...',
                text: 'Employee file is safe',
                // footer: '<a href="">Why do I have this issue?</a>'
              })
        }
    })
    // let user_id = id;
   
}

function getData(id, lastname, firstname, email, username, password) {
    $('#id').val(id);
    $('#lastname').val(lastname);
    $('#firstname').val(firstname);
    $('#email').val(email);
    $('#username').val(username);
    $('#password').val(password);
}

// $('#update_form').on('submit', function(e){
//     e.preventDefault();

//     var form_data = new FormData($(this)[0]);

//     console.log(form_data);

//     $.ajax({
//         url: base_url+'dashboard/edit_Profile',
//         type: 'post',
//         // data: {userid:user_id},
//         data:form_data,
//         cache: false,
//         async: false,
//         processData: false,
//         contentType: false,
//         dataType: 'json',
//         success:function(data) {
//             Swal.fire(
//                 'Update!',
//                 'Employee has been updated successfully',
//                 'success'
//                 ).then(function(){
//                 window.location.reload(base_url+'dashboard/index');
//             })
//         },
//         error:function(data){

//         }

//     }) 

// })

function updateDetail() {
    let user_id =  document.getElementById('id').value;
    let lastname = document.getElementById('lastname').value;
    let firstname = document.getElementById('firstname').value;
    let username = document.getElementById('username').value;
    let email = document.getElementById('email').value;
    let psw = document.getElementById('password').value;

    Swal.fire({
        title: 'Are you sure?',
        text: "You won't be able to revert this action!",
        icon: 'warning',
        showCancelButton: true,
        confirmButtonColor: '#3085d6',
        cancelButtonColor: '#d33',
        confirmButtonText: 'Yes, update it!'
        }).then((result) => {
        if (result.isConfirmed) {
            $.ajax({
                url: base_url+'dashboard/edit_Profile',
                type: 'post',
                // data: {userid:user_id},
                data:{userid: user_id, lname: lastname, fname: firstname, email: email, $uname: username, password: psw},
                cache: false,
                async: false,
                success:function(data) {
                    Swal.fire(
                        'Update!',
                        'Employee has been updated successfully',
                        'success'
                        ).then(function(){
                        window.location.reload(base_url+'dashboard/index');
                    })
                },
                error:function(data){

                }
        
            }) 
        }
        else{
            Swal.fire({
                icon: 'info',
                title: 'Yeah...',
                text: 'Employee file is safe',
                // footer: '<a href="">Why do I have this issue?</a>'
                })
        }
    })

}

//deactivate Employee

// $(document).ready(function() {
//     $(document).on('click', '#deactivate', function(){
//         //console.log('test')
//         var id = $(this).attr('data-id')
//         deactivate_Employee({userid: id})
//     })
// })

// function deactivate_Employee(data = {}) {
//     Swal.fire({
//         title: 'Are you sure?',
//         text: "You won't be able to revert this!",
//         icon: 'warning',
//         showCancelButton: true,
//         confirmButtonColor: '#3085d6',
//         cancelButtonColor: '#d33',
//         confirmButtonText: 'Yes, deactivate it!'
//       }).then((result) => {
//         if (result.isConfirmed) {
//             $.ajax({
//                 url: base_url+'dashboard/deactivate_Employee',
//                 type: 'post',
//                 // data: {userid:user_id},
//                 data: data,
//                 cache: false,
//                 async: false,
//                 success:function(data) {
//                     Swal.fire(
//                         'Deactivated!',
//                         'Employee has been deactivated.',
//                         'success'
//                       ).then(function(){
//                         window.location.reload(base_url+'dashboard/index');
//                     })
//                 }
        
//             }) 
//         }
//         else{
//             Swal.fire({
//                 icon: 'info',
//                 title: 'Yeah...',
//                 text: 'Employee status is safe',
//                 // footer: '<a href="">Why do I have this issue?</a>'
//               })
//         }
//     })
//     // let user_id = id;
   
// }

// Activate Employee

$(document).ready(function() {

    $(document).on('click', '.status-change', function(){
        let status = $(this).attr('status');
        let id = $(this).attr('data-id');

        let content = status==0?`<button class="btn btn-success status-change" status="1" data-id="${id}" data-toggle="tooltip" data-placement="top" title="Deactivate this employee" id="deactivate"><span class="fas fa-unlock"></span></button>`:`<button class="btn btn-danger status-change" status="0" data-id="${id}" data-toggle="tooltip" data-placement="top" title="Deactivate this employee" id="deactivate"><span class="fas fa-lock"></span></button>`;
        
        let data = {id:id, status:status}
        $(this).parent().prepend(content);
        $(this).remove();

        $.ajax({
            url: base_url+'dashboard/changeemployeeStatus',
            type: 'post',
            // data: {userid:user_id},
            data: data,
            cache: false,
            async: false,
            dataType: 'json',
            success:function(data) {
                
                if(data.message == 'a'){
                    Swal.fire(
                        'Activated!',
                        'Employee has been Activated.',
                        'success'
                        ).then(function(){
                        window.location.reload(base_url+'dashboard/index');
                    })
    
                }else{
                    Swal.fire(
                        'Deactivated!',
                        'Employee has been Deactivated.',
                        'success'
                        ).then(function(){
                        window.location.reload(base_url+'dashboard/index');
                    })
    
                }
             
            }
    
        }) 

        console.log(status)
        console.log(id)
    })

    // $(document).on('click', '#activate', function(){
    //     //console.log('test')
    //     var id = $(this).attr('data-id')
    //     activate_Employee({userid: id})
    // })
})

function activate_Employee(data = {}) {
    Swal.fire({
        title: 'Are you sure?',
        text: "You won't be able to revert this!",
        icon: 'warning',
        showCancelButton: true,
        confirmButtonColor: '#3085d6',
        cancelButtonColor: '#d33',
        confirmButtonText: 'Yes, Activate it!'
      }).then((result) => {
        if (result.isConfirmed) {
            $.ajax({
                url: base_url+'dashboard/do_upload',
                type: 'post',
                // data: {userid:user_id},
                data: data,
                cache: false,
                async: false,
                success:function(data) {
                    Swal.fire(
                        'Activated!',
                        'Employee has been activated.',
                        'success'
                      ).then(function(){
                        window.location.reload(base_url+'dashboard/index');
                    })
                }
        
            }) 
        }
        else{
            Swal.fire({
                icon: 'info',
                title: 'Yeah...',
                text: 'Employee status is still deactivated',
                // footer: '<a href="">Why do I have this issue?</a>'
              })
        }
    })
    // let user_id = id;
   
}


$(document).on('submit', '#uploadfile',function(e){
    e.preventDefault()
    var data = new FormData($(this)[0])
    uploaddisfile(data)
    // console.log(data);
})
function uploaddisfile(data = {}){
    $.ajax({
        url: base_url+'dashboard/do_upload',
        type: 'post',
        data: data,
        cache: false,
        async: false,
        processData: false,
        contentType: false,
        success:function(response) {
            console.log(response)
        },
        error: function(error){
            console.log(error)
        }
    })
}