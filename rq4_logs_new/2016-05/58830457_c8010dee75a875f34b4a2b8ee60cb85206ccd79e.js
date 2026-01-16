import React, { Component } from 'react'

export default class CommentForm extends Component {
	render() {
		return(
			<form className = "comment-form" onSubmit = { this.handleSubmit.bind(this) }>
				<label>New comment</label>
				<div className ="comment-form-fields">
					<input placeholder = "Name:" ref = {(input) => this.author = input} />
					<textarea placeholder =" Comment:" ref = {(input) => this.text = input }> </textarea>
				</div>
				<div className = "comment-form-actions">
					<button type ='submit'>
						Post Comment
					</button>
				</div>
			</form>
			)
	}

	handleSubmit(event) {
		event.preventDefault();

		this.props.addComment(this.author.value, this.text.value);

		this.author = ''
		this.text = ''
	}
}