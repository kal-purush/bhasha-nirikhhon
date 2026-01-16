type Method = 'GET' | 'DELETE' | 'HEAD' | 'OPTIONS' | 'POST' | 'PUT' | 'PATCH' | 'PURGE' | 'LINK' | 'UNLINK';

export default async function sendRequest(action: string, method: Method, data: object): Promise<any> {
    return await fetch('https://zpacovanimrazek.8u.cz/', {
        headers: {
            'Content-Type': 'application/json'
        },
        method: method,
        body: JSON.stringify(data),
    }).then(response => response.json()).then(result => {
        return result
    }).catch(responseError => {
        console.error('Error', responseError)
        console.warn('You propably have error in your BE method returned data is not valid JSON')
    })

}

