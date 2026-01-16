import React from 'react'
import RaisedButton from 'material-ui/RaisedButton'

const AuthButton = (props) => {
    if (props.authenticated){
        return (
            <RaisedButton
            label="logout"
            onTouchTap={props.auth.logout}
            fullWidth={true}
            secondary
            />
        )
    }else{
        return(
            <RaisedButton
            label="login/Signup"
            onTouchTap={props.auth.showLock}
            fullWidth={true}
            primary
            />
        )
    }
}


export default AuthButton