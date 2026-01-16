import React from 'react';

const styles = {
    center: {
        position: "absolute",
        width: 200,
        height: 100,
        top: "50%",
        left: "50%",
        marginLeft: -100,
        marginTop: -50
    }
}
class Page404 extends React.Component {
    render() {
        return (
            <h1 style={styles.center}>404 ไม่เจอ</h1>
        )
    }
}

export default Page404