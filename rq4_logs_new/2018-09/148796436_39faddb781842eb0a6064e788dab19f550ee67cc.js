import React from 'react';

const withOtherClass = (WrappedComponent, className) => {
    return (props) => {
        return (
        <div className={className}>
            <WrappedComponent />
        </div>
        );
    };
}


export default withOtherClass;