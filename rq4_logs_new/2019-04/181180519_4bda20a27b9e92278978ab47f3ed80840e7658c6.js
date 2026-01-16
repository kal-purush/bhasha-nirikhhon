import React, { Component } from 'react';
import axios from 'axios'
import { connect } from 'react-redux';
import { getCampuses } from './store'

class AddCampus extends Component {
    constructor() {
        super()
        this.state = {
            name: '',
            address: '',
            description: '',
            disabled: false,
        }
        this.handleSubmit = this.handleSubmit.bind(this)
        this.handleChange = this.handleChange.bind(this)
    }

    handleChange = (obj) => {
        // console.log(obj.target.name)
        this.setState({ [obj.target.name]: obj.target.value }, () => {
            this.setState({ disabled: !!this.state.name && !!this.state.address })
            console.log(this.state.name, this.state.address, this.state.disabled)
        })
    }

    handleSubmit = (event) => {
        event.preventDefault()
        const { name, address, description } = this.state
        // this.setState({ disabled: !!name && !!address })
        axios.put('/api/campuses', { name, address, description })
            .then((newCampus) => newCampus.data)
            // .then(() => )
            // .then((newCampus) => this.props.history.push(`/campuses/${newCampus.id}`))
            .then(() => this.props.fetchCampuses())
            .catch(er => console.error(er))
    }
    render() {
        const { name, address, description, disabled } = this.state
        return (
            <form onSubmit={this.handleSubmit}>
                <h3>Add New Campus</h3>
                <div >
                    <label htmlFor="name">
                        Campus Name <input onChange={this.handleChange} value={name} type="text" name="name" />
                    </label>
                    <div />
                    <label htmlFor="address">
                        Address <input onChange={this.handleChange} value={address} type="text" name="address" />
                    </label>
                    <div />
                    <label htmlFor="description">
                        Description <input onChange={this.handleChange} value={description} type="text" name="description" />
                    </label>
                </div>
                <button type="submit" disabled={!disabled}> Create Campus</button>
            </form>
        )
    }
}
const mapDispatchToProps = dispatch => ({
    fetchCampuses: () => dispatch(getCampuses())
})
export default connect(null, mapDispatchToProps)(AddCampus);