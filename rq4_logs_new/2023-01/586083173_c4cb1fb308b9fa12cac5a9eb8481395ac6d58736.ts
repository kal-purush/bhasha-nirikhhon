import { useCallback, useMemo } from 'react';
import { Alert } from 'react-native';
import {
    AuthenticationDetails,
    CognitoUser,
    CognitoUserPool,
    CognitoUserSession
} from 'amazon-cognito-identity-js';
import { useDispatch } from 'react-redux';
import { setUserStateAction } from '@store/UserReducer';

export const useCognito = (): {
    register: (firstname: string, username: string, password: string) => void;
    login: (username: string, password: string) => void;
} => {
    const dispatch = useDispatch();

    const USER_POOL_ID = 'eu-central-1_kaF0fBM87';
    const CLIENT_ID = '213lat3j0sc0c1gcta8iqqtb4r';

    const cognitoPool = useMemo(
        () =>
            new CognitoUserPool({
                UserPoolId: USER_POOL_ID,
                ClientId: CLIENT_ID
            }),
        []
    );

    const register = useCallback(
        (firstname: string, username: string, password: string) => {
            if (!username) {
                return Alert.alert('Please provide username');
            }
            if (!password) {
                return Alert.alert('Please provide password');
            }
            if (password.length < 8) {
                return Alert.alert(
                    'Password is too short, safer would be at least 8 characters'
                );
            }
            return cognitoPool.signUp(username, password, [], null, (err) => {
                if (err) {
                    switch (err.name) {
                        case 'InvalidPasswordException':
                            return Alert.alert(
                                'Sorry, the password is invalid'
                            );
                        case 'UsernameExistsException':
                            return Alert.alert(
                                'Sorry, this username already exists'
                            );
                        default:
                            return Alert.alert(
                                'Oop, sorry, something went wrong'
                            );
                    }
                } else {
                    return dispatch(
                        setUserStateAction({ firstname, username })
                    );
                }
            });
        },
        [cognitoPool, dispatch]
    );

    const login = useCallback(
        (username: string, password: string) => {
            if (!username) {
                return Alert.alert('Please provide username');
            }
            if (!password) {
                return Alert.alert('Please provide password');
            }
            const user = new CognitoUser({
                Username: username,
                Pool: cognitoPool
            });
            const authDetails = new AuthenticationDetails({
                Username: username,
                Password: password
            });
            return user.authenticateUser(authDetails, {
                onSuccess: (res: CognitoUserSession) => {
                    if (res) {
                        dispatch(setUserStateAction({ username }));
                    }
                },
                onFailure: (err) => {
                    if (err) {
                        Alert.alert('Sorry, username or password is incorrect');
                    }
                }
            });
        },
        [cognitoPool, dispatch]
    );

    return { register, login };
};