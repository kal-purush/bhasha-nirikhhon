const axios = require("axios")
// Change port to be dynamic,
let baseurl = "http://localhost:4200/api/groups/";

async function axiosGet(url:string, _data:any){
        try{
                let res= await axios.get(baseurl+url, {params: _data})
                return { status:res.status, data:res.data}
        }
        catch(err){
                return {status:err.response.status, data:err.response.data}
        }
}
async function axiosPost(url:string, _data:any){
        try{
                let res= await axios.post(baseurl+url,   _data , {headers:{'Content-Type':'application/json'}})
                return { status:res.status, data:res.data}
        }
        catch(err){
                return {status:err.response.status, data:err.response.data}
        }
}

async function getGroup(id:number){
        return await axiosGet("getgroup", {id:id})
}

async function newGroup(userid:number, _name:string, _info:string){
        return await axiosPost("new", {id:userid, name:_name, info:_info })
}
async function removeGroup(userid:number, _groupid:number){
        return await axiosPost("remove", {userid:userid, id:_groupid })
}

async function updateGroup(userid:number, _groupid:number, _name:string, _info:string){
        return await axiosPost("update", {userid:userid, id:_groupid , name:_name, info:_info})
}

async function addmembers(userid:number, groupid:number, memberid:number){
        return await axiosPost("addmembers", {userid:userid, group:groupid ,member:memberid})
}

async function removemembers(userid:number, groupid:number, memberid:number){
        return await axiosPost("rmmembers", {userid:userid, group:groupid ,member:memberid})
}

export {axiosGet, axiosPost, getGroup, newGroup, removeGroup, updateGroup, addmembers, removemembers}
