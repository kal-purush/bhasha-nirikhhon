import * as types from '../constants/ActionTypes';

export function addPost(text) {
	return { type: types.ADD_POST, text };
}

export function likeUnlikePost(id) {
	return { type: types.LIKE_UNLIKE_POST, id };
}

export function addComment(id, commentText) {
	return { type: types.ADD_COMMENT, id, commentText };
}