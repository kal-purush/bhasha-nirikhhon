console.log("Hello outsider, i'm from console.log")



const form = document.querySelector('form');
const search = document.querySelector('.search');
const message1 = document.querySelector('.message1');
const message2 = document.querySelector('.message2');

form.addEventListener('submit',(e) =>{
    e.preventDefault();
    const location = search.value;

    message1.innerHTML = 'Loading...';
    message2.innerHTML = '';
    fetch(`http://localhost:3000/weather?address=${location}`).then((response)=>{
    response.json().then((data) =>{
        if(data.error){
            message1.innerHTML = data.error;
            
        } else{
            message1.innerHTML = data.location;
            message2.innerHTML = data.forecastReport;
        }
      
    })
})

})