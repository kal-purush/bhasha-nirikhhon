import React from 'react';



class CreateTodoList extends React.Component{
	constructor(props){
		super(props);

		this.state = {
			error: null
		}
	}

	renderError(){
		if(!this.state.error){return null;}	
		return <div style={{color: 'red'}}>{this.state.error}</div>;
	}
	
	render(){
		return (
			<form onSubmit={this.handleCreate.bind(this)}>
				<input type="text" placeholder="What do I need to do?" ref="createInput"/>
				<button>Create</button>
				{this.renderError()}
			</form>

		);
	}

	handleCreate(event){
		event.preventDefault();

		const createInput = this.refs.createInput;
		const task = createInput.value;
		const valideteInput = this.valideteInput(task);

		if(valideteInput) {
			this.setState({error:valideteInput});
			return;
		}
		this.setState({error: null});

		this.props.createTask(task);
		this.refs.createInput.value = '';
	}
	valideteInput(task){
		if(!task){
			return 'Please enter a task.'
		}else if (_.find(this.props.todos, todo => todo.task === task)) {
			return 'Task already exists.';
		}else {
			return null;
		}

	}
}

export default CreateTodoList;