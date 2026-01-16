/**
 * Created by Porter on 12/28/2017.
 */
import React, {Component} from 'react';

class HelpText extends Component {

    render() {
        //console.log(this.props.users)
        let displayClass = this.props.display ? "cardsTitle" : "hidden";
        return (
            <div className={displayClass}>
                {this.props.html}
            </div>
        )
    }
}

export default HelpText;