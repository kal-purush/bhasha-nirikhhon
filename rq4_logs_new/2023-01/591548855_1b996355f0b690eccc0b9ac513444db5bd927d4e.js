import { useHistory } from 'react-router-dom';
import { useSelector } from 'react-redux';
import React, { useState, useEffect } from 'react';
import axios from 'axios';

function Review() {

  const history = useHistory();
  
  const feeling = useSelector((store) => store.feeling);
  const understanding = useSelector((store) => store.understanding);
  const support = useSelector((store) => store.support);
  const comments = useSelector((store) => store.comments);

  const handleSubmit  = () => {
    axios({
      method: 'POST',
      url: '/feedback',
      data: {
        feeling,
        understanding,
        support,
        comments
      }
    }).then((res) => {
      history.push('/submitted');
    }).catch((err) => {
      console.error('Error in Review POST: ', err);
    })
  };

  return (
    <>
      <table>
      <tbody>
          <tr>
            <td>Feeling</td>
            <td>{feeling}</td>
          </tr>
          <tr>
            <td>Understanding</td>
            <td>{understanding}</td>
          </tr>
          <tr>
            <td>Support</td>
            <td>{support}</td>
          </tr>
          <tr>
            <td>Comments</td>
            <td>{comments}</td>
          </tr>
        </tbody>
      </table>
      <button onClick = {handleSubmit}>Submit Feedback</button>
    </>
  )
}

export default Review;