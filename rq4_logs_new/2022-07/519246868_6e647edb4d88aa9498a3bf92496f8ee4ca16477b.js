import React from "react";
import { Route, Navigate} from "react-router-dom";

const PrivateRoute = ({component: Component, ...rest}) => {
    console.log("route props", rest) //route props
    return <Route {...rest} //route props
            render={(props) => {
                if (localStorage.getItem('token')) {
                    //user is authenticated - render the component
                    return <Component {...props} {...rest}/>;
                } else {
                    //user is NOT authenticated - redirect to login
                    return <Navigate to="/login"/>
                }
            }} 
            />
}

export default PrivateRoute;