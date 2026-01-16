class postServices {
    fetchPosts = async () => {
        const response = await fetch('https://boopeepapir.onrender.com/posts');
        return response.json();
    }

    fetchUsers = async () => {
        const response = await fetch('https://boopeepapir.onrender.com/users');
        return response.json();
    }

    fetchComments = async () => {
        const response = await fetch('https://boopeepapir.onrender.com/comments');
        return response.json();
    }

    fetchImages = async () => {
        const response = await fetch('https://boopeepapir.onrender.com/images');
        return response.json();
    }
}

export default postServices