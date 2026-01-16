import React from 'react';

interface AlertInterface {
    message: string | null;
    type: 'success' | 'error' | 'warning' | 'info';
    visible: boolean;
}

const useAlert = () => {
    const [alert, setAlert] = React.useState<AlertInterface>({
        message: null,
        type: 'info',
        visible: false,
    });

    const showAlert = (
        message: string,
        type: 'success' | 'error' | 'warning' | 'info' = 'info'
    ) => {
        setAlert({
            message,
            type,
            visible: true,
        });

        setTimeout(() => {
            hideAlert();
        }, 3000);
    };

    const hideAlert = () => {
        setAlert({
            message: null,
            type: 'info',
            visible: false,
        });
    };

    return { alert, showAlert, hideAlert };
};

export default useAlert;