import React, { createContext, useState } from "react";
import { Alert } from "react-bootstrap";
import { AlertsWrapper } from "./AlertsWrapper";
import "./style.css";

const AlertsContext = createContext();
const AlertsProvider = ({ children }) => {
    const [alerts, setAlerts] = useState([]);

    const agregarAlert = (alert) => {
        const id = Math.random().toString(36).slice(2, 9) + new Date().getTime().toString(36);
        setAlerts((prev) => [{ ...alert, id }, ...prev]);
    }

    const eliminarAlert = (id) => {
        const index = alerts.findIndex((alert) => alert.id === id);
        const newAlerts = [...alerts.slice(0, index), ...alerts.slice(index + 1)];
        setAlerts(newAlerts);
    }

    return (
        <AlertsContext.Provider value={{ agregarAlert }}>
            <AlertsWrapper show={alerts.length > 0}>
                {alerts.map((alert) => (
                    <Alert
                        key={alert.id}
                        className="AlertContext-alert"
                        onClose={() => eliminarAlert(alert.id)}
                        variant={alert.severidad}
                        dismissible
                    >
                        {alert.icon} {' '}
                        {alert.mensaje}
                    </Alert>
                ))}
        </AlertsWrapper>
            { children }
        </AlertsContext.Provider >
    );
}

export { AlertsContext, AlertsProvider };