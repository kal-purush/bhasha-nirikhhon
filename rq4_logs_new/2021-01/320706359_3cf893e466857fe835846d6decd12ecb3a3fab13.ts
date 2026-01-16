import { AccordionActions, WithStyles, Theme } from '@material-ui/core'
import actions from '../store/actions'
import { connect } from 'react-redux'
import { AnyAction, Dispatch } from 'redux'
import {AddPostScreen } from './AddPostScreen'
import { State } from '../store/store'
import { Styles, StyledComponentProps } from '@material-ui/core/styles/withStyles'


const addPostToMarketplace = (post: Post): AnyAction => {
    debugger;
    return {
        type: actions.ADD_POST,
        postToAdd: post
    }
}

const mapStateToProps = (state: State): AddPostScreenProps => {
    return ({
        postings: state.applicationState.posts
    } as unknown) as AddPostScreenProps;
}

const mapDispatchToProps = (dispatch: Dispatch): AddPostScreenProps => 
    (({
        addPost: (post: Post) => {
            dispatch(addPostToMarketplace(post));
        },
    } as unknown) as AddPostScreenProps);

const styles: Styles<Theme, StyledComponentProps> = (theme) => ({})

export interface AddPostScreenProps extends WithStyles<typeof styles> {
    posts: Post[];
}

export default connect(mapStateToProps, mapDispatchToProps)(AddPostScreen);