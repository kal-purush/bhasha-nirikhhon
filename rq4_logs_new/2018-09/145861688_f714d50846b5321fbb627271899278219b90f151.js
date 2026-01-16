import React, { Component } from "react"
import { Card, CardText, CardImg, CardBody, CardTitle, Container } from 'reactstrap'
import axios from "axios"
import { Redirect } from "react-router-dom";
class Detail extends Component {
  constructor(props) {
    super(props);
    this.state = {}

  }
  componentDidMount() {
    axios({
      method: 'GET',
      url: `/api/detail/${this.props.match.params.id}`,
      headers: { 'X-SimpleOvpApi': sessionStorage.getItem('user_key') }
    }).then(response => {
      this.setState({ data: response.data.items[0] })
    })
  }
  render() {
    if (!sessionStorage.getItem('user_key')) {
      return <Redirect from="/" to="/login" />
    }

    else if (!this.state.data) {
      return <div>Loading</div>
    }
    else {
      let data = this.state.data
      return (
        <Container>
          <Card>
            <CardImg src={`/${data.imageSrc}`} />
            <CardBody>
              <CardTitle>{data.title}</CardTitle>
              <CardText>{data.description}</CardText>
              <CardText>ImDB rating: {data.rating}</CardText>
              <CardText>Genres: {data.genre}</CardText>
              <CardText>Length: {data.length}</CardText>
              <CardText>Release Date: {data.releaseDate}</CardText>
              <CardText>
                Trailer:
                <br />
                <video controls>
                  <source src={data.videoSrc} type="video/mp4" />
                  Your browser does not support the video tag.
                </video>
              </CardText>
            </CardBody>
          </Card>
        </Container>
      )
    }
  }
}
export default Detail