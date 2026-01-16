import { createSelector } from 'reselect'

const postCommentsSelector = (state: any) => state.posts.currentPost && state.posts.currentPost.comments

const postCommentFilter = createSelector(
    postCommentsSelector,
    items => console.log(items)
)

export { postCommentFilter }