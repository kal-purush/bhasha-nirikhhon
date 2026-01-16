const form=document.getElementById("itinerary-form")
const itinerary=document.getElementById("itinerary")
form.addEventListener("submit",async (e)=>{
    e.preventDefault();
    //collect data from form
    
    const data={'destination':form.destination.value,
        'pax':form.persons.value,
        'dept_date':form.departure_date.value,
        'arrival_date':form.arrival_date.value,
        'type_of_trip':form.journey_type.value,
        'budget':form.budget.value,
    };

    //console.log('data=',data)
    //sending data to server
    try{
        const response=await fetch('http://127.0.0.1:8000/api/submit_data/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json', // This ensures the server knows it's JSON
            },
            body: JSON.stringify(data), // Ensure formData is serialized to JSON
        })
        console.log("status of post is",response.status)

    }
    catch(error){
        console.log("An error occurred, Unable to submit data",error)
    }
    setTimeout(()=>{
        fetch('http://127.0.0.1:8000/api/get_itinerary/')
        .then(response=>response.json())
        //.then(data=>console.log('returned json is=',data))
        .then(data=>{
            itinerary.innerHTML=data.itinerary
        })
    },2000)//fetching response from server
    

})