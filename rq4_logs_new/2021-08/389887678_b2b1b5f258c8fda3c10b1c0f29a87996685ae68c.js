import Header from "./Header";
import { Link } from "react-router-dom";
import { Form, Button } from "react-bootstrap";
import React, {useState, useEffect} from "react";
import { useHistory } from "react-router";

const AddAdmin = ()=>{

    const [name, setName] = useState("");
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");
    const [phone, setPhone] = useState("");
    const history = useHistory();

    async function adminAdd() {
        let item = {name, email, password, phone};
        let result = await fetch("http://127.0.0.1:8000/api/addAdmin",{
            method:"POST",
            headers:{
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            body: JSON.stringify(item)
        });
        result = await result.json();
        let message;
        console.log(result);
        if(result.error && result.error.length>0){
            message = '<div class="alert alert-danger" role="alert"><strong> <span className="glyphicon glyphicon-ok-sign"></span> </strong>'+result.error+'</div>';
            document.getElementById("message").innerHTML = message;
        }else{
            console.log(result);
            history.push("/adminList");
        }
    }

    return (
        <div>
            <Header/>
            <div className="container-fluid">
                <h2>Add Admin</h2>
                
                <br/>
                <Link to="adminList" className="btn btn-info">Admin List</Link>
                <br/><hr/>
                <div className="panel panel-primary">
                    <div className="panel-heading">
                        <h3 className="panel-title">Admin Information</h3>
                    </div>
                    <div className="panel-body">
                        <div id="message"></div>
                        <div className="form-group">
                            <label for="email">Email</label>
                            <input type="text"onChange={(e)=>setEmail(e.target.value)} className="form-control" value={email} name="email" id="email" />
                        </div>
                        <div className="form-group">
                            <label for="password">Password</label>
                            <input type="text"onChange={(e)=>setPassword(e.target.value)} className="form-control" value={password} name="password" id="password" />
                        </div>
                        <div className="form-group">
                            <label for="name">Name</label>
                            <input type="text"onChange={(e)=>setName(e.target.value)} className="form-control" value={name} name="name" id="name" />
                        </div>
                        <div className="form-group">
                            <label for="name">Phone</label>
                            <input type="text"onChange={(e)=>setPhone(e.target.value)} className="form-control" value={phone} name="phone" id="phone" />
                        </div>
                        
                        <input type="submit" onClick={adminAdd} value="Add Admin" className="btn btn-primary" />
                    </div>
                </div>
            </div>
        </div>
    )

}

export default AddAdmin;