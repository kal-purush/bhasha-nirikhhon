import React from 'react';

function owo(e) {
    e.preventDefault()

    
}

function CreateAccount() {
    return (
        <div className="container">
            <div className="row">
            <div className="col-md"/>
            <form className="col-md-8">
                <div className="form-group row">
                    <label for="exampleInputEmail1" className="col-sm-2 col-form-label">Email address</label>
                    <div className="col-sm-10">
                        <input type="email" className="form-control" id="exampleInputEmail1" aria-describedby="emailHelp" placeholder="Enter email"/>
                    </div>
                </div>
                <div className="form-group row">
                    <label for="exampleInputPassword1" className="col-sm-2 col-form-label">Password</label>
                    <div className="col-sm-10">
                        <input type="password" className="form-control" id="exampleInputPassword1" placeholder="Password"/>
                    </div>
                </div>

                <button type="submit" className="btn btn-primary" onClick={owo}>Submit</button>
            </form>
            <div className="col-md"/>
            </div>
        </div>
    )
}

export default CreateAccount;