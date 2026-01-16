import React, { Component } from 'react';
import { connect } from 'react-redux';
import { loadSingleIMage } from '../redux/actions/imagesActions';

class PostDetailesPage extends Component {
  constructor(props) {
    super(props);

    this.state = {
      postData: {
        source: '',
        image_url: ''
      }
    }
  };

  componentWillMount() {
    const postId = this.props.location.pathname.split("/post/")[1];
    this.props.loadSingleIMage(postId)
      .then(response => response.json())
      .then(response => {
        const postData = {
          source: '',
          image_url: ''
        }
        postData.source = response.source;
        postData.image_url = response.image_url;
        this.setState({postData})
      })
  }
  
  render() {
    return (
      <div className="container" >
        <div className="container__content" >
          <div className="items" >
            <div className="image-wrapper" >
              <img  src={this.state.postData.image_url} alt=""/>
            </div>
            <div>
            </div>
          </div>
          <div className="items" >
            <div className="form-wrapper" >
            </div>
          </div>
        </div>
      </div>
    )
  }
}

const mapStateToProps = (state) => {
  return {
    images: state.images
  }
};

export default connect(mapStateToProps, { loadSingleIMage })(PostDetailesPage);