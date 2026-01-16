export const Car = ({value}) => {
    return (
        <ul>
            <li>{value.id}</li>
            <li>{value.model}</li>
            <li>{value.price}</li>
            <li>{value.year}</li>
        </ul>

    );
}