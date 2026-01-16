import React from 'react'


function WrappedComponent(WrappedComponent1, WrappedComponent2, countMore) {
    return class extends React.Component {
        render() {
            return  countMore>=this.props.views?  <WrappedComponent1 {...this.props}/> :  <WrappedComponent2 {...this.props}/>
        }
    }
}

export default WrappedComponent