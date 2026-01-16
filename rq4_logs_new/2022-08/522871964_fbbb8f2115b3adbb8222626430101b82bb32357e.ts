import {ChangeEvent, FormEvent, useState} from "react";
import { useNavigate } from 'react-router-dom';
import {useLazyQuery, useMutation} from "@apollo/client";
import {CREATE_USER, GET_USER_BY_EMAIL} from "../../queries/user";

export const useLogin = () => {
    const navigate = useNavigate();

    const emailRegex = /^[a-zA-Z0-9]+@[a-zA-Z0-9]+\.[A-Za-z]+$/
    const [email, setEmail] = useState('');
    const [error, setError] = useState('');

    const [getUserByEmail, {data, loading, refetch}] = useLazyQuery(GET_USER_BY_EMAIL);
    const [createUser, _] = useMutation(CREATE_USER);


    const onLogin = async (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        if (email === '') {
            setError('Please enter email address!');
            return
        } else if (!isValidEmail()) {
            setError('Not a valid email address!')
            return
        }

        if (await userExists()){
            login()
        } else {
            register()
        }
    }

    const userExists = async () => {
        const result = await fetchUser()
        return result.data.getUserByEmail !== null;
    }

    const fetchUser = async () => {
        return await getUserByEmail({
            variables: {
                email
            }
        });
    }

    const login = () => {
        navigate('/list')
    }

    const register = async () => {
        await createUser({
            variables: {
                email
            }
        }).then(_ => navigate('/list'))
        .catch(error => alert(error.message));
    }

    const handleInput = (event: ChangeEvent<HTMLInputElement>) => {
        setError('');
        setEmail(event.target.value);
    }

    const isValidEmail = () => {
        return emailRegex.test(email)
    }

    return {
        onLogin, handleInput, email, error
    }
}