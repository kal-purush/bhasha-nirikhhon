import React, { Component } from 'react'
import Form from 'react-bootstrap/Form'
export class UpdateBook extends Component {
    constructor(props) {
        super(props)
        this.state = {
            updatedBookName: '',
            updatedBookDesc: '',
            updatedBookStatus: ''
        }
    }
    getBookName = (e) => {
        this.setState({
            updatedBookName: e.target.value
        })
    }
    getBookDesc = (e) => {
        this.setState({
            updatedBookDesc: e.target.value
        })
    }
    getBookStatus = (e) => {
        this.setState({
            updatedBookStatus: e.target.value
        })
    }
    getBookInfo = (e) => {
        e.preventDefault();
        this.props.handleUpdateBookInfo(this.state.updatedBookName, this.state.updatedBookDesc, this.state.updatedBookStatus)

    }
    render() {
        return (
            <div>
                <Form onSubmit={this.getBookInfo}>
                    <Form.Group>
                        <h3>Update BOOk: </h3>
                        <Form.Label>Book Name</Form.Label>
                        <Form.Control type="text" placeholder={this.props.recievedBookName} onChange={this.getBookName} required/>
                        <Form.Label>Book Description</Form.Label>
                        <Form.Control type="text" placeholder={this.props.recievedBookDesc} onChange={this.getBookDesc}required />
                        <Form.Label>Book Status</Form.Label>
                        <Form.Control type="text" placeholder={this.props.recievedBookStatus} onChange={this.getBookStatus} required />
                    </Form.Group>
                    <input value="Update" type='submit' />
                </Form>
            </div>
        )
    }
}

export default UpdateBook