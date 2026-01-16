import React, { Component } from 'react';
import axios from 'axios';
import 'react-datepicker/dist/react-datepicker.css';
import Container from '../Container';

import AppContext from '../../appContext';

export default class CreateServices extends Component {
    constructor(props) {
        super(props);

        this.onChangeName = this.onChangeName.bind(this);
        this.onChangeCategory = this.onChangeCategory.bind(this);
        this.onChangeDescription = this.onChangeDescription.bind(this);
        this.onChangeDuration = this.onChangeDuration.bind(this);
        this.onChangePrice = this.onChangePrice.bind(this);
        this.onSubmit = this.onSubmit.bind(this);

        this.state = {
            name: '',
            category: '',
            description: '',
            duration: 0,
            price: 0,
            isCreated: props.isCreated
        };
    }

    onChangeName(e) {
        this.setState({
            name: e.target.value
        });
    }

    onChangeCategory(e) {
        this.setState({
            category: e.target.value
        });
    }

    onChangeDescription(e) {
        this.setState({
            description: e.target.value
        });
    }

    onChangeDuration(e) {
        this.setState({
            duration: e.target.value
        });
    }

    onChangePrice(e) {
        this.setState({
            price: e.target.value
        });
    }

    onChangeDate(date) {
        this.setState({
            date: date
        });
    }

    onSubmit(e) {
        e.preventDefault();

        const service = {
            name: this.state.name,
            category: this.state.category,
            description: this.state.description,
            duration: this.state.duration,
            price: this.state.price,
            providerId: this.props.providerId
        };

        console.log(service);

        axios.post('/api/services', service).then(res => console.log(res.data));
        this.setState({
            name: '',
            category: '',
            description: '',
            duration: 0,
            price: 0,
            isCreated: true
        });

        this.props.handleClick();
    }

    render() {
        return (
            <Container>
                {!this.state.isCreated ? (
                    <div>
                        <h4>Post A Service Below</h4>
                        <hr />
                        <br />

                        <form onSubmit={this.onSubmit}>
                            <div className="form-group">
                                <label>
                                    <h5>Name:</h5>
                                </label>
                                <input
                                    type="text"
                                    required
                                    className="form-control"
                                    value={this.state.name}
                                    onChange={this.onChangeName}
                                />
                            </div>

                            <div className="form-group">
                                <label>
                                    <h5>Category:</h5>
                                </label>
                                <select
                                    id="category"
                                    name="category"
                                    className="form-control"
                                    defaultValue="type"
                                    onChange={this.onChangeCategory}
                                >
                                    <option value="type" disabled>
                                        choose
                                    </option>
                                    <option value="make-up">Make-Up</option>
                                    <option value="hair-barber">Hair/Barber</option>
                                    <option value="yoga">Yoga Instructor</option>
                                    <option value="massage">Massage</option>
                                    <option value="fitness">Fitness Trainer</option>
                                    <option value="nutritionist">Nutritionist</option>
                                    <option value="fashion">Fashion Stylist</option>
                                    <option value="nails">Manicure</option>
                                </select>
                            </div>
                            <br />

                            <div className="form=group">
                                <label>
                                    <h5>Description:</h5>
                                </label>
                                <textarea
                                    type="text"
                                    required
                                    className="form-control"
                                    value={this.state.description}
                                    onChange={this.onChangeDescription}
                                />
                            </div>
                            <br />

                            <div className="form-group">
                                <label>Duration (in minutes): </label>
                                <input
                                    type="text"
                                    required
                                    className="form-control"
                                    value={this.state.duration}
                                    onChange={this.onChangeDuration}
                                />
                            </div>

                            <div className="form-group">
                                <label>Price $$: </label>
                                <input
                                    type="text"
                                    className="form-control"
                                    value={this.state.price}
                                    onChange={this.onChangePrice}
                                />
                            </div>

                            <div className="form-group">
                                <input
                                    type="submit"
                                    value="Create Service"
                                    className="btn btn-primary"
                                />
                            </div>
                        </form>
                    </div>
                ) : (
                    <div>
                        <h2>created!</h2>
                    </div>
                )}
            </Container>
        );
    }
}

CreateServices.contextType = AppContext;