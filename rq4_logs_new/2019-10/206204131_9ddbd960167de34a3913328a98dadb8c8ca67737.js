import React, { Component } from 'react';
import styled from 'styled-components';

function getChildHeight(child,parent){
    const childClone = child.cloneNode(true);
    parent.appendChild(childClone);
    childClone.style.height = 'auto';
    const height = childClone.clientHeight;
    parent.removeChild(childClone);
    return height;
}

const FrameContainer = styled.div`
    border: solid 3px black;
    overflow:hidden;
    height:0;
    transition: 0.7s;
    &.activated {
        height:${props=>{console.log('render Frame');return props.frameHeight;}}px !important;
    }
`

const Pane = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`

class Frame extends Component {
    state={
        hideSpinner:false,

    }
    componentDidUpdate(prevProps){
        if(prevProps.isLoading && !this.props.isLoading){
            this.setState({
                hideSpinner:true
            })
        }
        if(prevProps.products.length === 0 && this.props.products.length>0){
            const insuranceFrame = document.getElementById('insurance');
            const wrapper = document.getElementById('wrapper')
            const frameHeight = getChildHeight(insuranceFrame,wrapper);
            console.log('update frame height')           
            this.setState({frameHeight}, ()=>{
                console.log('callback and activate frame')
                insuranceFrame.classList.add('activated');
            });
        }
    }
    render () {
        console.log('rendering');
        return (
            // <React.Fragment id='wrapper'>
                <div id='wrapper'>
                    <div style={{visibility:this.state.hideSpinner?'hidden':'visible'}}>
                        I'm the spinner...I'll disappear after 3 secs
                    </div>
                    <br/>
                    {this.props.products.length >0 && 
                        <FrameContainer id='insurance' frameHeight={this.state.frameHeight}>
                            <Pane style={{backgroundColor: 'skyblue',height:'150px'}}>
                            {this.props.products[0]}
                            </Pane>
                            <Pane style={{background:'lightgray', height:'15px'}}>
                                Another line
                            </Pane>
                            <Pane style={{backgroundColor: 'orange',height:'150px'}}>
                                {this.props.products[1]}
                            </Pane>
                        </FrameContainer>}
                </div>
        );
    }
}

export default Frame;