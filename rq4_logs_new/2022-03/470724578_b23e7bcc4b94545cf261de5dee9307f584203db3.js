import { useParams } from "react-router-dom";

const BlogDetails = () => {
    const {id} = useParams() // in App.js we define path => /blogs/:id ===so===> the id need to be a variable so we use useParams and mention id that we assign that in app.js to set links dynamic and also use it in <h2></h2>

    return(
        <div className="blog-details">
        <h2>Blog Details - {id}</h2>
        </div>
    );
}
export default BlogDetails