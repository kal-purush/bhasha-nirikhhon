
console.log("It is Working Nirmith")
quer= "America"
let b = fetch(`https://restcountries.eu/rest/v2/all`)
.then(res => res.json())
.then((user)=>{
    return user
})

let arr=[]
function emptyInput(){
    fetch(`https://restcountries.eu/rest/v2/all`)
    .then(res => res.json())
    .then((user)=>{
        console.log(user)
       let output =""
       console.log(user.length)
       let o = document.getElementById("ooo")
       for(i=0;i<user.length;i++){
           output += `
           <div class="outCont" >
                <p id="countryName">${user[i].name}</p>
                <div style="display: flex;justify-content: center;padding: 20px;">
                    <img class="image" src=${user[i].flag} alt=${user[i].name} width="160px" height="110px" style="border-radius: 10px;">
                </div>
                <p id="countryPopulation"><b>Population:</b> ${user[i].population}</p>
                <p id="countryCapital"><b>Capital:</b> ${user[i].capital}</p>
                <p id="countryAlpha"><b>Alpha Code:</b> ${user[i].alpha3Code}</p>
                
            </div>
           `
       }
       o.innerHTML = output
    })
}

function DisplayWrongInput(){
    console.log("THIS IS THE NO DISPLAY COMMITEE")
    out = document.getElementById("ooo")
    out.innerHTML = '<p id="contFail">THERE IS NO COUNTRY THAT MATCHES THE DESCRIPTION</p>'
}

document.addEventListener("DOMContentLoaded",function(){
    //when the page gets loaded we render that all countries by default or when user inputs in nothing
    emptyInput()
    //console.log(arr)

    //when user starts inputting stuff we find it when it matches the name ya dig?
    
    let k = document.getElementById("Submit")
    k.addEventListener("click",() => {
        console.log("YAY")
        let inp = document.getElementById("searchBox")
        console.log(inp.value)
        console.log(inp.value.length)
        if(inp.value.length==0){
            //if output no there display a text saying the user's input doesn't match any country -> later add on        
            emptyInput()
        }
        else{
            fetch(`https://restcountries.eu/rest/v2/name/${inp.value}`)
            .then(res=> {
                if (res.status >= 200 && res.status <= 299) {
                    return res.json();
                  } else {
                    //console.log("ERROR")
                    DisplayWrongInput()
                  }
            })
            .then((user)=>{
                //console.log(user)
                let output =""
               // console.log(user.length)
                let o = document.getElementById("ooo")
                for(i=0;i<user.length;i++){
                   output += `
                   <div class="outCont" >
                        <p id="countryName">${user[i].name}</p>
                        <div style="display: flex;justify-content: center;padding: 20px;">
                            <img class="image" src=${user[i].flag} alt=${user[i].name} width="160px" height="110px" style="border-radius: 10px;">
                        </div>
                        <p id="countryPopulation"><b>Population:</b> ${user[i].population}</p>
                        <p id="countryCapital"><b>Capital:</b> ${user[i].capital}</p>
                        <p id="countryAlpha"><b>Alpha Code:</b> ${user[i].alpha3Code}</p>
                    </div>
                    `
                }
                o.innerHTML = output
            })
            .catch((err) =>{
                //console.log(err)
                alert("Failure In Getting Country")
            })
        }
    })
    
    

})