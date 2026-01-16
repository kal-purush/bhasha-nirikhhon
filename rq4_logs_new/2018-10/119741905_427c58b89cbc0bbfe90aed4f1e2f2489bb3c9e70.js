import React from 'react';
import {Button} from '@material-ui/core';

class Component extends React.Component {
    render() {
        let {id, label, color, onClick, size,variant,disabled} = this.props;

        return <div className={"centered-element-container"}>
            <Button
                id={id}
                color={color}
                onClick={onClick}
                variant={variant}
                disabled={disabled}
                style={{
                    width: size
                }}
            >
                {label}
            </Button>
        </div>;
    }
}

export default Component;