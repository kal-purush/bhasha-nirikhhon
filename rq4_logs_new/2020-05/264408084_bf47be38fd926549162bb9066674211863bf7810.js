import React,{useState,useEffect} from 'react';
import { Router, Route, NavLink } from 'react-router-dom';
import { history, Role } from '@/_helpers';
import { authenticationService } from '@/_services';
import { PrivateRoute } from '@/_components';
import { Inventory } from '@/Inventory';
import { HrSection } from '@/HrSection';
import { Settings } from '@/Settings';
import { Finance } from '@/Finance';
import { LoginPage } from '@/LoginPage';

function App(props){
    const [currentUser,setCurrentUser]= useState(null);
    const [isAdmin,setIsAdmin] =useState(false);
    const logout= ()=> {
        authenticationService.logout();
        history.push('/login');
    }
    useEffect(function(){
                authenticationService.currentUser.subscribe(x => {
                    setCurrentUser(x); 
                    console.log(currentUser);
                    setIsAdmin(x && x.role === Role.Admin);
                }
            );
        },[]);

    return (
        <Router history={history}>
            <div>
                {currentUser && currentUser !=null &&
                <div>
                    <div className="header-top">
                        <a onClick={logout} className="nav-item">logout</a>
                        <a className="nav-item">{currentUser.firstName}</a>
                    </div>
                    <nav className="nav-header">
                        <NavLink exact to="/" className="nav-item">HR</NavLink>
                        {isAdmin ? <NavLink to="/finance" className="nav-item">Finance</NavLink> : 
                        <span className="nav-item">Finance</span>}
                        <NavLink to="/inventory" className="nav-item">Inventory</NavLink>
                        <NavLink to="/settings" className="nav-item">Settings</NavLink> 
                    </nav>
                </div>
                }

                <div>
                    <PrivateRoute exact path="/" component={HrSection} />
                    <PrivateRoute exact path="/hr" component={HrSection} />
                    <PrivateRoute path="/finance" roles={[Role.Admin]} component={Finance} />
                    <PrivateRoute exact path="/inventory" component={Inventory} />
                    <PrivateRoute exact path="/settings" component={Settings} />
                    <Route path="/login" component={LoginPage} />
                </div>
            </div>
        </Router>
    );
}
export { App }; 