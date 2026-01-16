import React, {Component} from "react";
import DictData from '../data/data.json'

class DictList extends Component{
    render() {
        return(
            <div>
                <h1>Hello There</h1>
                {DictData.map((entryData, index)=>{
                    return <div> 
                        <h1>Pleonasmo #{entryData.id}</h1>
                        <p>{entryData.firstWord} {entryData.secondWord}</p>
                    </div>
                })}
            </div>
        )
    }
}

export default DictList;