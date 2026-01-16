import Skeleton from "../components/skeleton/Skeleton";
import ErrorMessage from "../components/errorMessage/ErrorMessage";
import Spinner from "../components/spinner/Spinner";

const setContent = (process, Component, data) => {
    switch (process) {
        case 'waiting':
            return <Skeleton />;
            break;
        case 'loading': 
            return <Spinner />;
            break;
        case 'error':
            return <ErrorMessage />;
            break
        case 'confirmed': 
            return <Component data={data} />
        default:
            throw new Error('Unexpected process state')
    }
}

export default setContent