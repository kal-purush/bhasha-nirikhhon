import React, { Component } from 'react';
import { withAuth0 } from '@auth0/auth0-react';
import axios from 'axios';
import { Col, Container, Row, Card, Button } from 'react-bootstrap';
import UpdateModel from './UpdateModel';

class FavWatch extends Component {
    constructor(props) {
        super(props);
        this.state = {
            favWatchList: [],
            watchId: '',
            title: '',
            image_url: '',
            description: '',
            toUSD: '',
            isOpen: false,
        }
    }

    handleClose = () => this.setState({ isOpen: false });
    handleShow = () => this.setState({ isOpen: true });

    componentDidMount = async () => {
        let url = `${process.env.REACT_APP_HEROKU}/getUser/${this.props.auth0.user.email}`;
        let user = await axios.get(url);
        this.setState({
            favWatchList: user.data.favWatchs
        });
        this.forceUpdate();
    }

    handleDeleteFav = async (watchId) => {
        let url = `${process.env.REACT_APP_HEROKU}/deleteFav/${this.props.auth0.user.email}/${watchId}`;
        let user = await axios.delete(url);
        this.setState({
            favWatchList: user.data.favWatchs
        });
        this.forceUpdate();
    }
    handleUpdateFav =  (watchId, title, image_url, description, toUSD) => {
        this.setState({
            watchId: watchId,
            title: title,
            image_url: image_url,
            description: description,
            toUSD: toUSD,
            isOpen: true,
        })
        this.forceUpdate();
    }
    handleTitle = (event) => {
        this.setState({
            title: event.target.value
        })
    }
    handleImage_url = (event) => {
        this.setState({
            image_url: event.target.value
        })
    }
    handleDescription = (event) => {
        this.setState({
            description: event.target.value
        })
    }
    handleToUSD = (event) => {
        this.setState({
            toUSD: event.target.value
        })
    }
    handleUpdateSubmit = async (watchId) => {
        let url = `${process.env.REACT_APP_HEROKU}/updateFav/${this.props.auth0.user.email}/${watchId}`;
        let updateData = {
            watchId: this.state.watchId,
            title: this.state.title,
            image_url: this.state.image_url,
            description: this.state.description,
            toUSD: this.statetoUSD,
        }
        let user = await axios.put(url, updateData);
        this.setState({
            favWatchList: user.data.favWatchs,
            isOpen: false
        })
        this.forceUpdate();
    }
    render() {
        return (
            <>
                <UpdateModel
                    isOpen={this.isOpen}
                    handleUpdateSubmit={this.handleUpdateSubmit}
                    handleToUSD={this.handleToUSD}
                    handleTitle={this.handleTitle}
                    handleImage_url={this.handleImage_url}
                    handleDescription={this.handleDescription}
                    handleClose={this.handleClose}
                    watchId={this.state.watchId}
                    title={this.state.title}
                    image_url={this.state.image_url}
                    description={this.state.description}
                    toUSD={this.statetoUSD}
                    handleShow={this.handleShow}

                />
                <Container>
                    <Row xs={1} md={3} className="g-3">
                        {this.state.favWatchList.map((elem, index) => {
                            return (
                                <Col key={index}>
                                    <Card style={{ width: '18rem' }}>
                                        <Card.Img variant="top" src={elem.image_url} />
                                        <Card.Body>
                                            <Card.Title>{elem.title}</Card.Title>
                                            <Card.Text>{elem.description}</Card.Text>
                                            <Card.Text>{elem.toUSD}</Card.Text>
                                            <Button onClick={() => this.handleDeleteFav(elem._id)} variant="danger">Delete</Button>
                                            <Button onClick={() => this.handleUpdateFav(elem._id, elem.title, elem.image_url, elem.description, elem.toUSD)} variant="info">Update</Button>
                                        </Card.Body>
                                    </Card>
                                </Col>
                            )
                        })
                        }
                    </Row>
                </Container >
            </>
        )
    }
}

export default withAuth0(FavWatch)