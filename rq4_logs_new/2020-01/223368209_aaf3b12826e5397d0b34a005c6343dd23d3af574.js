import React from 'react';
import CopyRightFooter from '../../Websites/Lotus/CopyRightFooter'
import FormRegular from './FormRegular';
import PostData from './PostData';



const AddPost = () => {
  return (
    <>
      <div className="apps">
        <h1>Add Post - UseState [hook]</h1>
        <p>Source : https://jsonplaceholder.typicode.com/posts</p>

        <FormRegular />
        <PostData />

      </div>
      <CopyRightFooter />
    </>
  )
}

export default AddPost