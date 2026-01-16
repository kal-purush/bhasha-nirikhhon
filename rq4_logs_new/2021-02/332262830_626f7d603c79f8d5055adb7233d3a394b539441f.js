import React, { useState } from "react";
import { useMutation } from "@apollo/react-hooks";
import { ADD_POST } from "../utils/mutations";
import { QUERY_POSTS, QUERY_ME } from "../utils/queries";
import { Link, useHistory } from "react-router-dom";
import ReactLoading from "react-loading";

const AddAudioPost = (postMediaType) => {
  const history = useHistory();
  let mediaType = postMediaType.postMediaType;
  const [audioUrl, setAudioUrl] = useState("");
  const [audioFormState, setAudioFormState] = useState({
    postMediaType: mediaType,
    postDescription: "",
    postLink: "",
    postPrimaryMedia: "",
    postSecondaryMedia: "",
    postPaywall: true,
  });

  const [audio, setAudio] = useState("");

  // NOTE: Changed to mine for now to test but make sure that it is fixed by the end of it
  const postAudioDetails = () => {
    const loadingBar = document.getElementById("loadingBar");
    loadingBar.style.visibility = "visible";
    const data = new FormData();
    data.append("file", audio);
    data.append("upload_preset", "post-audio");
    data.append("cloud_name", process.env.CLOUD_NAME);
    fetch("https://api.cloudinary.com/v1_1/creative-square/video/upload", {
      method: "post",
      body: data,
    })
      .then((res) => res.json())
      .then((data) => {
        loadingBar.style.visibility = "hidden";
        setAudioUrl(data.secure_url);
      })
      .catch((err) => {
        console.log(err);
      });
  };

  const [image, setImage] = useState("");
  const [imageUrl, setImageUrl] = useState("");

  const postImageDetails = () => {
    const loadingBar = document.getElementById("loadingBar");
    loadingBar.style.visibility = "visible";
    const data = new FormData();
    data.append("file", image);
    data.append("upload_preset", "post-image");
    fetch("https://api.cloudinary.com/v1_1/creative-square/image/upload", {
      method: "post",
      body: data,
    })
      .then((res) => res.json())
      .then((data) => {
        loadingBar.style.visibility = "hidden";
        setImageUrl(data.secure_url);
      })
      .catch((err) => {
        console.log(err);
      });
  };

  const [addPost, { error }] = useMutation(ADD_POST, {
    update(cache, { data: { addPost } }) {
      try {
        const { posts } = cache.readQuery({ query: QUERY_POSTS });
        cache.writeQuery({
          query: QUERY_POSTS,
          data: { posts: [addPost, ...posts] },
        });
      } catch (err) {
        console.error(err);
      }

      const { me } = cache.readQuery({ query: QUERY_ME });
      cache.writeQuery({
        query: QUERY_ME,
        data: { me: { ...me, posts: [addPost, ...me.posts] } },
      });
    },
  });

  const handleChange = (event) => {
    const { name, value } = event.target;
    setAudioFormState({
      ...audioFormState,
      [name]: value,
    });
  };

  const handleFormSubmit = async (event) => {
    event.preventDefault();

    try {
      // add post to database
      await addPost({
        variables: {
          ...audioFormState,
          postPrimaryMedia: audioUrl,
          postSecondaryMedia: imageUrl,
        },
      });

      return history.push("/homefeed");
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div className="bg-white px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
      <div className="sm:flex sm:items-start">
        <div className="mt-3 text-center sm:mt-0 sm:ml-4 sm:text-left">
          <h2 className="text-2xl text-center">Create a Post!</h2>
          <div className="w-full">
            <div className="font-bold h-6 mt-3 text-gray-600 text-xs leading-8 uppercase">
              <span className="text-red-400 mr-1">*</span> Upload Audio File
            </div>
            <div className="my-2 p-1 flex rounded">
              {" "}
              <input
                className="p-1 appearance-none outline-none w-full text-gray-800"
                name="postPrimaryMedia"
                type="file"
                onChange={(e) => setAudio(e.target.files[0])}
              />{" "}
              <button
                className="btn bg-green-900 rounded text-gray-200 px-3"
                type="submit"
                onClick={() => postAudioDetails()}
              >
                Upload
              </button>
            </div>
            {audioUrl !== "" ? (
              <audio controls>
                <source alt="audio-preview" src={audioUrl} type="audio/mpeg" />
              </audio>
            ) : (
              <div id="loadingBar" style={{ visibility: "hidden" }}>
                <ReactLoading type={"bars"} color={"grey"} />
              </div>
            )}
          </div>
          <div className="font-bold h-6 mt-3 text-gray-600 text-xs leading-8 uppercase">
            <span className="text-red-400 mr-1">*</span> Upload Image
          </div>
          <div className="my-2 p-1 flex rounded">
            {" "}
            <input
              className="p-1 appearance-none outline-none w-full text-gray-800"
              name="postPrimaryMedia"
              type="file"
              onChange={(e) => setImage(e.target.files[0])}
            />{" "}
            <button
              className="btn bg-green-900 rounded text-gray-200 px-3"
              type="submit"
              onClick={() => postImageDetails()}
            >
              Upload
            </button>
          </div>
          {imageUrl !== "" ? (
            <img
              className="preview-img"
              alt="profile-pic-preview"
              height="200px"
              width="200px"
              src={imageUrl}
            />
          ) : (
            <div id="loadingBar" style={{ visibility: "hidden" }}>
              <ReactLoading type={"bars"} color={"grey"} />
            </div>
          )}

          <form onSubmit={handleFormSubmit}>
            <div className="w-full">
              <div className="font-bold h-6 mt-3 text-gray-600 text-xs leading-8 uppercase">
                <span className="text-red-400 mr-1">*</span> Post Description
              </div>
              <div className="my-2 bg-white p-1 flex border border-gray-200 rounded">
                {" "}
                <input
                  className="p-1 px-2 appearance-none outline-none w-full text-gray-800"
                  name="postDescription"
                  type="text"
                  value={audioFormState.postDescription}
                  onChange={handleChange}
                />{" "}
              </div>
            </div>
            <div className="w-full">
              <div className="font-bold h-6 mt-3 text-gray-600 text-xs leading-8 uppercase">
                <span className="text-red-400 mr-1">*</span> External URL
              </div>
              <div className="my-2 bg-white p-1 flex border border-gray-200 rounded">
                {" "}
                <input
                  className="p-1 px-2 appearance-none outline-none w-full text-gray-800"
                  name="postLink"
                  type="text"
                  value={audioFormState.postLink}
                  onChange={handleChange}
                />{" "}
              </div>
            </div>
            <div className="w-full">
              <div className="font-bold h-6 mt-3 text-gray-600 text-xs leading-8 uppercase">
                <span className="text-red-400 mr-1">*</span> post paywall
              </div>
              <div className="my-2 bg-white p-1 flex border border-gray-200 rounded">
                {" "}
                <input
                  name="postPaywall"
                  type="text"
                  value={audioFormState.postPaywall}
                  onChange={handleChange}
                />{" "}
              </div>
            </div>
            <div className="mt-6 relative">
              <button
                type="submit"
                className="mb-1 w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-blue-600 text-base font-medium text-white hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500 sm:ml-3 sm:w-auto sm:text-sm"
              >
                ADD POST
              </button>
              <Link to="/">
                <button
                  type="cancel"
                  className="mt-4 w-full inline-flex justify-center rounded-md border border-gray-300 shadow-sm px-4 py-2 bg-white text-base font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm"
                >
                  Cancel
                </button>
              </Link>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
};

export default AddAudioPost;