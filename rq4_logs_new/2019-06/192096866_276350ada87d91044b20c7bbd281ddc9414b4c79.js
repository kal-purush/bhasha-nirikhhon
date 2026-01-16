import React from 'react';
import axios from 'axios';

//axios concatenates /users to base url below
//allows you to change your api route in single spot
//process.env.REACT_APP_API_URL 
//ENV variables 

//auth0  get out of way asap!!!!
//
axios.defaults.baseURL = 'http://localhost:3500/api';

//interceptors are like mw for axios for every response
//allows us to require passing of tokens for every components
//works only for components rendering with routers
//headers always exist b/c axios adds headers by default 
//any requests will have token attached auto matically 
//any auth or things configurable with axios you have access to
//config object below is what axios has or what you passed yourself (comes with axios)

//intercepters code just needs to be ran before token is required

axios.interceptors.request.use(
    function(requestConfig) {
        requestConfig.headers.authorization = localStorage.getItem('jwt');
//returns new version of config with aquth
        return requestConfig;
    },

    function(error){
        return Promise.reject(error)
    }
)

export default function(Component) {
    //takes control of rendering in react is component below
    return class Authenticated extends React.Component{
render() {
   
    const NotLoggedIn = <h3>Please login to see the jokes</h3>
    const token = localStorage.getItem('jwt');

    return  <> { token? <Component {...this.props}/> : NotLoggedIn}  </>;
}
    };
} 