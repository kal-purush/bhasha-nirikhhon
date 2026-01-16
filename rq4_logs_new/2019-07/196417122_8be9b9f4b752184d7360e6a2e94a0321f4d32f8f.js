import React from 'react'

 export class Toast extends React.Component {
    render() {
        const divStyle = {
            position: 'fixed',
            bottom: '50px',
            left: '20px',
            backgroundColor: 'green',
            color: '#fff',
            padding: '15px',
        }

         return (
            <div className="Toast-container">
                <div style={divStyle}>
                    Some message...
                </div>
            </div>
        )
    }
}

export default Toast 