import './home.css'
import Cookies from 'js-cookie';
import { useNavigate } from 'react-router-dom';
import React, { useEffect } from 'react';

function getCookie(cookieName) {
    return Cookies.get(cookieName);
}

async function login() {
    const fetchData = async () => {
        try {
            const response = await fetch(process.env.REACT_APP_PROXY_URL + "/api/v1/login/ok");
            if (response.ok) {
                return true;
            } else {
                throw new Error(response.status);
            }

        } catch (error) {
            console.error(error);
            return false
        }
    };

    return (await fetchData());
}

async function tryLogin() {
    var session = false
    while (1) {
        session = await login()
        if (session) {
            break;
        }
        await new Promise(r => setTimeout(r, 1000));
    }

    return session
}


export default function Home() {
    const navigate = useNavigate();
    const cookieName = process.env.REACT_APP_COOKIE_NAME;
    useEffect(() => {
        var cookie = getCookie(cookieName);
        if (cookie !== undefined) {
            console.log('Cookie exists!');
            navigate('/convert')
        } else {
            console.log('Cookie does not exist!');
            window.open(process.env.REACT_APP_PROXY_URL + "/api/v1/login/ok", '_blank');
            tryLogin()
        }
    }, [navigate, cookieName]);


    return (
        <>
            <div className="background-div-home">
                <h1>Welcome to imgConv!</h1>
                <h3>Your all in one image convertion service</h3>
            </div>
        </>
    );

}