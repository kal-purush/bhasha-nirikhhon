import React from 'react'
import { Navigate } from 'react-router-dom'
import { useAuthContext } from './AuthContext'

export default function PrivateRoute({ Component }) {
    const { isAuthenticated } = useAuthContext()

    // if (!isAuthenticated)
    //     return <Navigate to="/Auth/login" replace />

    return (
        <Component />
    )

}