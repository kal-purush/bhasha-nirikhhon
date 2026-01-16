import { useRouter } from "next/router";
import data from "../data";
import Header from "../components/Header";
import Footer from "../components/Footer";

const BlogPost = () => {
  const router = useRouter();
  const { id } = router.query;
  const post = data.find((post) => post.id === parseInt(id));

  if (!post) {
    return <p>Loading...</p>;
  }

  return (
    <div>
      <Header />
      <div className="container mx-auto px-4 py-8">
        <h1 className="text-4xl font-bold mb-4">{post.title}</h1>
        <p className="text-gray-600 mb-2">
          {post.date} by {post.author}
        </p>
        <p className="text-lg">{post.description}</p>
      </div>
      <Footer />
    </div>
  );
};

export default BlogPost;