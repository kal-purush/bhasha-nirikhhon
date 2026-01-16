import react, {useState, useEffect } from 'react';
import { Link } from 'react-router-dom';


function Page({ match }) {
    useEffect(() => {
        fetchBlogPage();
    }, [])

    const [data, setData] = useState([])


    const fetchBlogPage = async () => {
        const response = await fetch(`https://jsonplaceholder.typicode.com/todos/${match.params.id}`);
        const data = await response.json();
        setData(data);
    }


    return (
        <div className="blogPage-container">
            {
                <h1>{ data.title }</h1>
            }
        </div>
    );
}

export default Page;