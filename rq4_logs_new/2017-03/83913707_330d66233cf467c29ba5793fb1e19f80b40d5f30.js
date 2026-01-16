import React from 'react';
import '.././Notes.css'

export default class TopicListItem extends React.Component {
    constructor(props) {
        super(props);

        this.state = {
            isEditing: false
        };
    }

    render() {
        return (
            <div className="TopicLst">
            </div>
        );
    }
}