import { AccordionActions, WithStyles, Theme } from '@material-ui/core'
import actions from '../store/actions'
import { connect } from 'react-redux'
import { AnyAction, Dispatch } from 'redux'
import { AddPostScreen, AddPostScreenProps } from '../components/top-level-components/AddPostScreen'
import { State } from '../store/store'
import { Styles, StyledComponentProps } from '@material-ui/core/styles/withStyles'


const addPostToMarketplace = (post: Post): AnyAction => {
    return {
        type: actions.ADD_POST,
        postToAdd: post
    }
}

const mapStateToProps = (state: State): AddPostScreenProps => {
    return ({
        posts: state.applicationState.posts
    } as unknown) as AddPostScreenProps;
}

const mapDispatchToProps = (dispatch: Dispatch): AddPostScreenProps => 
    (({
        addPost: (post: Post) => {
            dispatch(addPostToMarketplace(post));
        },
    } as unknown) as AddPostScreenProps);


export default connect(mapStateToProps, mapDispatchToProps)(AddPostScreen);