import React, {Component} from 'react';

class TextInput extends Component {
    
    onTextChange(event) {
        const {stateProperty} = this.props;
        this.props.onTextChange(stateProperty, event.target.value);
    }

    renderInputControl() {
        if (this.props.rows || this.props.cols){
            return <textarea defaultValue={this.props.value}
                cols={this.props.cols}
                rows={this.props.rows}
                style={styles.textAreaStyle}
                onChange={this.onTextChange.bind(this)} />
        }

        return <input defaultValue={this.props.value}
                style={styles.inputStyle}
                onChange={this.onTextChange.bind(this)} />
    }

    render(){
        return (
            <div style={styles.rootStyle}>
                <div>
                    <label>
                        { this.props.label }
                    </label>
                </div>
                <div>
                    { this.renderInputControl() }
                </div>
            </div>
        )
    }
}

const styles = {
    rootStyle: {
        color: 'grey'

    },
    inputStyle: {
        width: '90%',
        fontSize: 18,
        marginTop: 5,
        marginBottom:10
    },
    textAreaStyle: {
        fontSize: 18,
        fontSize: 18,
        marginTop: 5,
        marginBottom:10
    }

};
export default TextInput;