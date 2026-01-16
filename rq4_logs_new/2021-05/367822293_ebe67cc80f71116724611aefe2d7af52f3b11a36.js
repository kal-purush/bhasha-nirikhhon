import React,{useContext} from 'react';
import SignUp from '../signup';
import ToDo from '../todo/todo-connected';
import {If} from 'react-if';
import {LogInContext} from '../../context/acl'; 

function Home (props){
    const logging = useContext(LogInContext);
    return(
        <>
        <If condition={!logging.loggedIn}>
        <SignUp/>
        </If>
        <If condition={logging.loggedIn}>
        <ToDo/>
        </If>
        </>
    );
}
export default Home;