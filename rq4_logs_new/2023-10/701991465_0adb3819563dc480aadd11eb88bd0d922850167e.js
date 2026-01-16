import error from './error.gif';
const ErrorMessage = () => {
    //<img src = {process.env.PUBLIC_URL + '/error.gif'}/>
    return (
        <img src={error} alt="Error" style={{display: 'block', width: '250px', height: '250px', objectFit: 'contain', margin: '0 auto'}}/>
    )
}

export default ErrorMessage;