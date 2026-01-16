import React from 'react'

  export default class Action extends React.Component{
    constructor(props){
      super(props);
      this.state = this.props
      this.actionFunction = this.actionFunction.bind(this);
  }


  actionFunction(event){
    if(event.keyCode === 38 && this.props.player1Pos > 0) {
      console.log('up')
      const {player1Pos} = this.props
          this.setState({
            player1Pos: player1Pos - 5
          })
        }  
        if(event.keyCode === 40 && this.props.player1Pos < this.props.height - this.props.playerHeight) {
          console.log('down')
          const {player1Pos} = this.props
              this.setState({
                player1Pos: player1Pos + 5
              })
            }  
            
        if(event.keyCode === 87 && this.props.player2Pos > 0) {
          console.log('w')
          const {player2Pos} = this.props
              this.setState({
                player2Pos: player2Pos - 5
              })
            }  

        if(event.keyCode === 83 && this.props.player2Pos < this.props.height - this.props.playerHeight) {
          console.log('s')
          const {player2Pos} = this.props
              this.setState({
                player2Pos: player2Pos + 5
              })
            }  
  }


  componentDidMount(){

    document.addEventListener("keydown", this.actionFunction, false);
  }

  componentWillUnmount(){
    document.removeEventListener("keydown", this.upFunction, false);
  }

  render(){
    return(
      <div></div>
    )
  }

  }