const axios = require("axios")
// Change port to be dynamic,
let baseurl = "http://localhost:4200/api/events/";

async function axiosGet(url:string, _data:any){
        return axios.get(baseurl+url, {params: _data})
}
async function axiosPost(url:string, _data:any){
        return axios.post(baseurl+url,   _data , {headers:{'Content-Type':'application/json'}})
}


async function newEvent(name:string, desc:string, address:string, time:Date, manager:number){
        return axiosPost("new", {Name:name,  Description:desc, Address:address, Time:time, Manager:manager})
        .then(ret => {  return ret.data })
        .catch(err => { return undefined})
        // .catch(err => {console.log(err); return err})
}

async function getEvent(_id:number){
        return axiosGet("get", {id:_id})
        .then(res => {
                if(res.data===''){
                        return undefined
                }
                else{
                        return res.data
                }
        })
        .catch(err =>  { {}} )

}

async function update(_id:number, _name:undefined|string, _desc:undefined|string, _address:undefined|string, _time:undefined|Date){
        // return axiosPost("update2", {id:_id, Name:_name,  Description:_desc, Address:_address, Time:_time, Manager:_manager})
        return axiosPost("update", {id:_id, Name:_name,  Description:_desc, Address:_address, Time:_time})
        .then(res => res.data)
        .catch(err => {undefined})
}

async function getAllEvents(){
        return axiosGet("getall", undefined)
        .then(res => res.data)
        .catch(err => { undefined})
}

async function Delete(_id:number){
        return axiosPost("delete", {id:_id})
        .then(res => {return true})
        .catch(err => false)
}

 /* @body {Event:EventId, Invited:[User1Id, User2Id]} */
async function Invite(_Event:number, _Users:[number]){
        return axiosPost("invite", {Event:_Event, Invited:_Users})
        .then(res => true)
        .catch(err => false)
}

 /*@body {Event:EventId, User:UserId} */
async function Signin(_Event:number, _User:number){
        return axiosPost("signin", {Event:_Event, User:_User})
        .then(res => true)
        .catch(err => {console.log(err); return false})
}

async function Transfer(_Event:number, _User:number ){
        return axiosPost("transfer",{Event:_Event, Manager:_User})
        .then(res => true)
        .catch(err => {console.log(err); return false})
}

export {axiosGet, axiosPost, newEvent, getEvent, update, getAllEvents, Delete, Invite, Signin, Transfer}
// module.exports= { login,new_user,reset_password, reset_token}