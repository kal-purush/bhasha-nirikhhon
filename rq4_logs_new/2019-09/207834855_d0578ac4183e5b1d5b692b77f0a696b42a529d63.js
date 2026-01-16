import React, {Component} from 'react'
import InfoRace from './infoRace/infoRace'
export default class main extends Component{
    constructor (){
        super()
        this.state ={
            load:false
        }
    }

    render(){
        return(
            <div>
                {this.props.infoId? <InfoRace raceId={this.props.infoId} />:null}
            </div>
        );
    }
}