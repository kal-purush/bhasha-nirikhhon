const creategroup = document.getElementById('creaegrpbtn')

creategroup.addEventListener('click', (e)=> {
    e.preventDefault()
    const create = document.getElementById('create')
    create.innerHTML += `
    <div> <input type="text" name="" id="msginput" placeholder="Enter Name">
    <button onclick=enterbtn()> Enter </button> 
    </div>`
})

function enterbtn(){
    const groupname= document.getElementById('msginput').value
    const grp = {
        groupname : groupname
    }
    const token = localStorage.getItem('token');
    axios.post('http://localhost:9000/chatroutes/creategroup', grp,  { headers: {"Authorization" : token}})
        .then(result => {           
            console.log(result)
            console.log("group created")
            alert('group created')
        })
        .catch(err => {
            console.log('Oh no what i did')
            console.log(err)
        })
}


window.addEventListener('DOMContentLoaded', ()=>{
    const token = localStorage.getItem('token');

    axios.get("http://localhost:9000/chatroutes/getgroup",  { headers: {"Authorization" : token}})
        .then(result => {
            console.log(result)
            localStorage.setItem('usergroup',JSON.stringify(result.data.response))
            // console.log(result.data.response[0].id)
            for(let i=0; i<result.data.response.length; i++){
                show(result.data.response[i].group.name,result.data.response[i].group.id)
            }
        })
})

function show(result,res){
    const showgrp = document.getElementById('showgrp')
    showgrp.innerHTML += `<div id="showgrp2"> ${result} - <button id="msgbtn" onclick="getmessages(${res})"> messages </button> - <button id='showusers' onclick=showusers(${res})> ShowUsers </button> </div>`
    // showgrp.style = "background-color:grey; padding:0.5rem; "
}

const logout = document.getElementById('logout')
logout.addEventListener('click', () => {
    if(confirm("do you reallt want to logout")){
        window.location = "login.html"
    }
})

function getmessages(res){
    const token = localStorage.getItem('token');
    axios.get(`http://localhost:9000/chatroutes/getmessages/${res}`, {headers: {"Authorization": token}})
        .then(result => {
            console.log(result)
            // console.log(result.data.result[1].user.name)
            let block = document.getElementById('block')
            block.innerHTML = " ";
            block.innerHTML += `<h3> Messages </h3>`
            block.innerHTML +=  "<button id='xbtn' onclick=Xbtn()> x </button>"
            // block.innerHTML +=  `<button id='showusers' onclick=showusers(${res})> ShowUsers </button>`
            for(let i=0;i<result.data.result.length;i++){
                block.innerHTML += `<div id="show">
                group ${result.data.result[i].groupId} -
                ${result.data.result[i].user.name} -
                ${result.data.result[i].messages}</div>`
            }
            block.style = `background-color: lightgrey;
            border: 2px solid black;
            padding: 5px;
            `
            let inputblock = document.getElementById('inputblock')
            inputblock.innerHTML = " "
            inputblock.innerHTML +=`
            <input type="text" name="" id="msginput" placeholder="Enter message">
            <button onclick=msgbtn(${res})>SEND</button>
            
            `
            inputblock.style = `background-color:grey;
            border: 2px solid black;
            padding: 20px`
        })
}

function Xbtn(){
    const block = document.getElementById('block')
    const inputblock = document.getElementById('inputblock')
    block.style = "display: none;"
    inputblock.style = "display: none;"
}

function msgbtn(res){
    const messages = document.getElementById('msginput').value
    const groupId = res
    const msg = {
        messages:messages
    }
    const token = localStorage.getItem('token');
    axios.post(`http://localhost:9000/chatroutes/messages/${groupId}`, msg, { headers: {"Authorization" : token} })
        .then(result => {      
            console.log(result)
            console.log("message sent")
        })
        .catch(err => {
            console.log('Oh no')
            console.log(err)
        })
}


function addusers(res){
    const name = document.getElementById('adduser').value

    const username = {
        name: name
    }
    const token = localStorage.getItem('token');
    axios.post(`http://localhost:9000/chatroutes/addusers/${res}`, username)
    .then(result => {
        console.log(result)
        alert(` User added`)
    })
    .catch(err => {
        console.log(err)
        alert("No user exist")
    })
}

function showusers(res){

    const token = localStorage.getItem('token');

    axios.get(`http://localhost:9000/chatroutes/getUsers/${res}`, { headers: {"Authorization" : token}})
        .then(response => {
            console.log(response)
            let Users = document.getElementById('block')
            Users.innerHTML = " "
            Users.innerHTML += "<h3> Users </h3>"

            const user = JSON.parse(localStorage.getItem('user'))
            const usergroups = JSON.parse(localStorage.getItem('usergroup'))
            // console.log(user[0].id)
            // console.log(usergroups)
            let usergroup;
            
            for(let i=0;i<usergroups.length;i++){
                if(res === usergroups[i].groupId){
                    usergroup = usergroups[i]
                    break;
                    }
            }
            console.log(usergroup)
            
                
                if(usergroup.isadmin == true){
                    for(let i=0; i<response.data.response.length;i++){
                        let id = response.data.response[i].user.id
                        if(user[0].id == id){
                            Users.innerHTML +=  " "
                            if(response.data.response[i].isadmin == true){
                                let inputblock = document.getElementById('inputblock')
                            inputblock.innerHTML = " "
                            inputblock.innerHTML +=`
                            <input type="text" name="" id="adduser" placeholder="Add User">
                            <button onclick=addusers(${res})> Add user </button>`
                            inputblock.style = `background-color:grey;
                            border: 2px solid black;
                            padding: 20px`
                            }
                        }
                        else{
                            if(response.data.response[i].isadmin == true){

                                Users.innerHTML += 
                                ` <ul> ${response.data.response[i].user.name} -  <button onclick=removeadmin(${res},${id}) > remove admin </button> - <button onclick=remove(${res},${id}) > remove user </button> </ul>`
                            }else{
                                Users.innerHTML += 
                                ` <ul> ${response.data.response[i].user.name} -  <button onclick=createadmin(${res},${id}) > create admin </button> - <button onclick=remove(${res},${id}) > remove user </button> </ul>`
                            }
                            Users.style = `background-color: LightGray; padding:20px; border: 2px solid black;`
                            let inputblock = document.getElementById('inputblock')
                            inputblock.innerHTML = " "
                            inputblock.innerHTML +=`
                            <input type="text" name="" id="adduser" placeholder="Add User">
                            <button onclick=addusers(${res})> Add user </button>`
                            inputblock.style = `background-color:grey;
                            border: 2px solid black;
                            padding: 20px`
                        }
                    }
                }
                else{
                    for(let i=0; i<response.data.response.length;i++){
                        let id = response.data.response[i].user.id
                        Users.innerHTML += 
                        ` <ul> ${response.data.response[i].user.name} - ${response.data.response[i].isadmin}</ul>`
                    }
                    Users.style = `background-color: LightGray; padding:20px; border: 2px solid black;`
                }
        })
        .catch(err => {
            console.log(err)
        })
}


function remove(res,id){
    const token = localStorage.getItem('token');
    axios.post(`http://localhost:9000/chatroutes/removeuser/${res}`, {id:id} )
        .then(response => {
            console.log(response)
            alert("removed")
        })
        .catch(err => {
            console.log(err)
        })
}

function createadmin(res, id){
    axios.post(`http://localhost:9000/chatroutes/createadmin/${res}`, {id:id})
        .then(response => {
            console.log("admin created")
            alert('admin created')
        })
        .catch(err => {
            console.log(err)
        })
}

function removeadmin(res, id){
    axios.post(`http://localhost:9000/chatroutes/rremoveadmin/${res}`, {id:id})
        .then(response => {
            console.log("admin created")
            alert('admin created')
        })
        .catch(err => {
            console.log(err)
        })
}