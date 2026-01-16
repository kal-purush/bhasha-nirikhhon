import {typeAction} from './type.action';
import {getComments, postComment, patchComment, deleteComment} from '../../services/comment.api.service';

export const getCommentsAction = (postid) => {
    return getComments(postid)
    .then(comments => {
        return {
            type: typeAction.GET_COMMENTS,
            payload: {
                comments
            }
        }
    })
}

export const postCommentAction = (id) => {
    return postComment(id)
    .then(res => {
        return {
            type: typeAction.POST_COMMENT
        }
    })
}

export const patchCommentAction = (id) => {
    return patchComment(id)
    .then(res => {
        return {
            type: typeAction.PATCH_COMMENT
        }
    })
}

export const deleteCommentAction = (id) => {
    return deleteComment(id)
    .then(res => {
        return {
            type: typeAction.DELETE_COMMENT
        }
    })
}