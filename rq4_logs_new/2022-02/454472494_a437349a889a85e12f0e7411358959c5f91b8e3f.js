import React from 'react'
import ClassList from './ClassList'

class viewClassContents extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            result: "",
            classes: ["bio", "math", "English", "history"]
        };
    }

    // classHandler(className) {
    //     const className = this.state.classes[this.state]
    // }

    render() {
        return(
            <div className="class-list-view">
                <ClassList
                result={this.state.result}
                    classClicked={this.classHandler.bind(this)}

                />
            </div>
        )
    }

    //functionality yet to be implemented
    favoriteHandler(){

    }
    searchHandler(){

    }


}

export default viewClassContents;