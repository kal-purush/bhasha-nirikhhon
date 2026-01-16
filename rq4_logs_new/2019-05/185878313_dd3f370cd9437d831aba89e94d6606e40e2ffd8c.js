import React from 'react';
import PropTypes from 'prop-types';

import PostHeader from '../postHeader/PostHeader';

import './style.scss';

const Post = props => (
  <div className="main-post">
    <PostHeader avatar={props.object.avatar} name={props.object.nome} hour={props.object.hora} />
    <p>{props.object.conteudo}</p>
  </div>
);

Post.prototype = {
  object: PropTypes.shape({
    id: PropTypes.number.prototype,
    nome: PropTypes.string.isRequired,
    avatar: PropTypes.string.isRequired,
    hora: PropTypes.string.isRequired,
    conteudo: PropTypes.string.isRequired,
  }).isRequired,
};

export default Post;