import React from 'react'
import { useDispatch, useSelector } from 'react-redux'
import useInput from '../../customhook/useInput';
import { updateNote } from '../../store/actions/noteAction';
import { useHistory } from 'react-router-dom';
const EditForm = () => {
    const note = useSelector((state) => state.note);
    console.log('edit form: ', note);
    const [title, bindTitle, resetTitle] = useInput(note.title);
    const [content, bindContent, resetContent] = useInput(note.content);
    const dispatch = useDispatch();
    const history = useHistory();
    const handleSubmit = (e) => {
        e.preventDefault()
        dispatch(updateNote({id: note.id, title, content }))
        resetTitle()
        resetContent()
        history.push('/')
    }
    return (
        <div className="section">
            <form onSubmit={handleSubmit} className="white">
                <h5 className="grey-text text-darken-3">New Note</h5>
                <div className="input-field ">
                    <input id="first_name" type="text" className="validate" {...bindTitle} />
                    <label htmlFor="note_title">Note Title</label>
                </div>
                <div className="input-field">
                    <textarea id="textarea1" className="materialize-textarea" {...bindContent}></textarea>
                    <label htmlFor="textarea1">Note Content</label>
                </div>
                <button className="btn green">Update</button>
            </form>
        </div>
    )
}

export default EditForm