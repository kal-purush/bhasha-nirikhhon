import {connectedReduxRedirect} from 'redux-auth-wrapper/history4/redirect';
import {openModal} from '../modals/modalActions'; 

export const UserIsAuthenticated = connectedReduxRedirect({
    wrapperDisplayName: 'UserIsAuthenticated',
    allowRedirectBack: true,
    redirectPath: '/blogs',
    authenticatedSelector: ({firebase: {auth}}) => auth.isLoaded && !auth.isEmpty, 
    redirectAction: newLocation => (dispatch) => {
        dispatch(openModal('UnauthModal'));
    }
});