import React from 'react'

class Task extends React.Component{
    constructor(props){
        super(props);
    }

    render(){
        return (
            <tr>
                <td>{this.props.task.title}</td>
                <td>{this.props.task.description}</td>
                <td><button>Edit</button><button>Delete</button></td>
            </tr>
        )
    }
}

export default Task