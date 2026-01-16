import PropTypes from 'prop-types';
import React from 'react';
import './PostsList.scss'

function PostsList({ handleClick, posts }) {
  if (posts.length > 0) {
    return posts.map((redditPost) => (
      <div
        className="post-preview-card"
        id={redditPost.id}
        key={redditPost.id}
        onClick={handleClick}
      >
        <img alt={redditPost.title} src={redditPost.thumbnail} />
        <span className="post-title">{redditPost.title}</span>
      </div>
    )
    );
  } else {
    return <h2>No posts matched your query!</h2>
  }
}

export default PostsList;

PostsList.propTypes = {
  handleClick: PropTypes.func.isRequired,
  posts: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.string.isRequired,
    thumbnail: PropTypes.string.isRequired,
    title: PropTypes.string.isRequired,
  }).isRequired)
}