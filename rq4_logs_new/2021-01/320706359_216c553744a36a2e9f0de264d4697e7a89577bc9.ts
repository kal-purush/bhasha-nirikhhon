import actions from '../store/actions'

const initialState: PostState = {
    posts: [
        {
            id: 1,
            title: "PS5",
            price: 1200.0,
            description: "Over priced second hand PS5."
        },
        {
            id: 2,
            title: "Xbox Series X",
            price: 500.0,
            description: "Not even upcharging, please take it off my hands."
        }
    ]
}

const reducer = (
        state: PostState = initialState,
        action: PostAction
    ): PostState => {
    switch(action.type) {
        case actions.ADD_POST:
            return {
                ...state,
                posts: state.posts.concat(action.post)
            }
        case actions.REMOVE_POST:
            const updatedPosts: Post[] = state.posts.filter(
                post => post.id !== action.post.id
            )
            return {
                ...state,
                posts: updatedPosts
            }
    }
    return state
}

export default reducer