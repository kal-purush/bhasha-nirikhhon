import React from 'react';
import './App.css';
import {BrowserRouter as Router, Route, withRouter} from 'react-router-dom';
import News from "./components/News/News";
import Music from "./components/Music/Music";
import Settings from "./components/Settings/Settings";
import DialogsContainer from "./components/Dialogs/DialogsContainer";
import UsersContainer from "./components/Users/UsersContainer";
import NavBarContainer from "./components/Navbar/NavBarContainer";
import ProfileContainer from "./components/Profile/ProfileContainer";
import HeaderContainer from "./components/Header/HeaderContainer";
import Login from "./components/Login/Login";
import {connect, Provider} from "react-redux";
import {compose} from "redux";
import {initializeAppThunk} from "./components/redux/appReducer";
import Preloader from "./components/common/Preloader/Preloader";
import store from "./components/redux/reduxStore";

class App extends React.Component {
    componentDidMount() {
        this.props.initializeAppThunk();
    }
    render() {
        if(!this.props.initialized){
            return <Preloader />
        }
        return (
                <div className='app-wrapper'>
                    <HeaderContainer/>
                    <NavBarContainer/>
                    <div className='app-wrapper-content'>
                        <Route path='/dialogs' render={() => <DialogsContainer/>}/>
                        <Route path='/profile/:userId?' render={() => <ProfileContainer/>}/>
                        <Route path='/users' render={() => <UsersContainer/>}/>
                        <Route path='/login' render={() => <Login/>}/>
                        <Route path='/news' render={() => <News/>}/>
                        <Route path='/music' render={() => <Music/>}/>
                        <Route path='/settings' render={() => <Settings/>}/>
                    </div>
                </div>
        );
    }
}

const mapStateToProps = (state)=>({
    initialized: state.app.initialized
});

// export default compose(
//     withRouter,
//     connect(mapStateToProps,{initializeAppThunk}))(App);

let AppContainer = compose(
    withRouter,
    connect(mapStateToProps,{initializeAppThunk}))(App);

const AppMainComponent = (props)=>{
    return (
        <Provider store={store}>
            <Router>
                <AppContainer/>
            </Router>
        </Provider>
    )
};
export default AppMainComponent;