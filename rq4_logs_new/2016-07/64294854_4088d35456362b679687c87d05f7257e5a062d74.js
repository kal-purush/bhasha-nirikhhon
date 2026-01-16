import React, {PropTypes, Component} from 'react';
import ReactDOM from 'react-dom';
import InfoCard from './InfoCard';

export default class Info extends Component {
  static propTypes = {
    data: PropTypes.array.isRequired
  };

  constructor(props) {
    super(props);
    this.state = {
      cardWidth: 0,
      infoWidth: 0
    }
  };

  setWidth() {
    let elem = ReactDOM.findDOMNode(this); // react 0.14 版本寫法
    let w = elem.parentNode.offsetWidth;
    // let h = elem.parentNode.offsetHeight;
    const currentWidth = this.state.infoWidth;
    if (w !== currentWidth) {
      this.setState({
        infoWidth: w,
        cardWidth: w / this.props.data.length
      });
    }
    //console.log(this.state);
  }

  componentDidMount() {
    window.addEventListener('resize', ::this.setWidth);
    this.setWidth();
  }

  componentWillReceiveProps() {
    this.setWidth();
  }

  componentWillUnmount() {
    window.removeEventListener('resize', ::this.setWidth);
  }

  render() {
    console.log(this.state);
    const {cardWidth} = this.state;
    let infoRender = [];
    infoRender = this.props.data.map(function(obj) {
      return (
        <InfoCard
          departureTime={obj.departureTime}
          duration={obj.duration}
          arriveTime={obj.arriveTime}
          width={cardWidth}
          key={obj.departureTime}
        />
      )
    });
    return (
      <div style={{display: 'flex', justifyContent: 'space-around', padding:'25px'}}>
        { infoRender }
      </div>
    )
  }
}