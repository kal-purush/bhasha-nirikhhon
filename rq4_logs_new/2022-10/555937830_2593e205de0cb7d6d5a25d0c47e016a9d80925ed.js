export const showCountyCreateBox =() =>{
    Swal.fire({
      title: 'Create county',
      html:
        '<input id="id" type="hidden">' +
        '<input id="county_name" class="swal2-input" placeholder="County Name" required="required">',
      focusConfirm: false,
      preConfirm: () => {
        
        countyCreate();
        return false
      }
    })
  }
  
  
export const countyCreate = ()=> {
    
    const county_name = document.getElementById("county_name").value;
    
    if (county_name) {
       
      
        const data ={
            "county_name":county_name,
            
        }
        fetch("http://localhost:5000/api/counties/create", {
            method: 'POST',
            body: new URLSearchParams(data),
            headers: new Headers({
                'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                "Access-Control-Allow-Origin": "*" 
            })
        })
        .then((response)=>response.json())
        .then(data=>{
            console.log(data)
            Swal.fire(data.message);
            loadCountiesTable();
        })
    }else{
        Swal.fire("Please fill the county Name")

    }

    
}

  export const showCountyEditBox = (id) =>{
    
    fetch("http://localhost:5000/api/counties/"+id)
    .then(response => response.json())
    .then(data=>{
        
        const county = data.data
      
        Swal.fire({
          title: 'Edit County',
          html:
            '<input id="id" type="hidden" value='+county['id']+'>' +
            '<input id="county_name" class="swal2-input" placeholder="County Name" value="'+county['county_name']+'">',
        
          focusConfirm: false,
          preConfirm: () => {
            countyEdit();
          }
        })

    })
 
  }
  
  export const countyEdit = ()=> {
    const id = document.getElementById("id").value;
    const county_name = document.getElementById("county_name").value;
     
    fetch("http://localhost:5000/api/counties/"+id+"/edit",{
        method :'PATCH',
        body:new URLSearchParams({
            "county_name":county_name,
            
        }),
        headers:new Headers({
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            "Access-Control-Allow-Origin": "*" 
        })
    })
    .then(response =>response.json())
    .then(data =>{
        Swal.fire(data.message);
        loadCountiesTable();
    })

  }

  export const countyDelete = (id) =>{

     
    fetch("http://localhost:5000/api/counties/"+id+"/delete",{
        method:"DELETE",
        headers: new Headers({
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            "Access-Control-Allow-Origin": "*" 
        })
    } )
    .then(response =>response.json())
    .then(data=>{
        Swal.fire(data.message);
        loadCountiesTable();
    })
    
  }
  export const loadCountiesTable = ()=> {
    fetch("http://localhost:5000/api/counties")
    .then(response=>response.json())
    .then(data=>{
        
        var trHTML = ''; 
        
        for (let object of data.data) {
           
          trHTML += '<tr>'; 
          trHTML += '<td>'+object['id']+'</td>';
        
          trHTML += '<td>'+object['county_name']+'</td>';  trHTML += '<td><button type="button" class="btn btn-outline-secondary edit-action" data-id="'+ object['id']+'">Edit</button>';
          trHTML += '<button type="button" class="btn btn-outline-danger delete-action"  data-id="'+ object['id']+'">Del</button></td>';
          trHTML += "</tr>";
          
        }
        document.getElementById("mytable").innerHTML = trHTML;
      
        const edit_buttons = document.querySelectorAll('.edit-action')
        
        edit_buttons.forEach(edit_button =>{
          edit_button.addEventListener('click',()=>{
            showCountyEditBox(edit_button.getAttribute('data-id'))
          })
        })
      
        const delete_buttons = document.querySelectorAll('.delete-action')
        
        delete_buttons.forEach(delete_button =>{
          delete_button.addEventListener('click',()=>{
            countyDelete(delete_button.getAttribute('data-id'))
          })
        })
     

    })
   
  }
  