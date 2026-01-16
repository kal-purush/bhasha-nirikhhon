import React, { useState } from 'react';

const AddCommentForm = ({articleName, setArticleInfo}) => {
    const [username, setUsername] = useState('');
    const [commentText, setCommentText] = useState('');

    const addComment = async (event) => {
        event.preventDefault();
        console.log(username + ' : ' + commentText);
        const result = await fetch(`/api/articles/${articleName}/add-comment`,{
            method:'post',
            body: JSON.stringify({username, text: commentText}),
            headers: {
                'Content-Type': 'application/json'
            }
        });
        const body = await result.json();
        setArticleInfo(body);
        setUsername('');
        setCommentText('');
    }

    return (
        <form onSubmit={addComment} id="add-comment-form">
            <legend>Comment Form</legend>

            <div className="form-group col-6">
                <label htmlFor="">Name:</label>
                <input type="text" value={username} onChange={(event) => setUsername(event.target.value)} className="form-control" id="" placeholder="Name" />
            </div>
            <div className="form-group col-6">
                <label htmlFor="">Comment:</label>
                <input type="text" value={commentText} onChange={(event) => setCommentText(event.target.value)} className="form-control" id="" placeholder="Comment" />
            </div>
            <button type="submit" className="btn btn-primary">Submit</button>
        </form>



                )
                
                
                
                }
                    
                    
export default AddCommentForm