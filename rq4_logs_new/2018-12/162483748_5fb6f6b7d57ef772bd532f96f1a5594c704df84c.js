import React,{ Component } from "react";

export default class UserName extends Component{

    constructor(props){
        super(props);

        this.state={
            name:''
        };
    }
    setName = (event)=>{

        event.target.value
    }
    render(){
        return (
            <div>
                <div>
                    <form onSubmit={}>
                        <label>Nombre de usuario:</label>
                        <input name='userName' onChange={this.setName} ref={(val)=>console.log(val)} />
                        <button>Usar este nombre</button>
                    </form>
                </div>
            </div>
        );
    }

}