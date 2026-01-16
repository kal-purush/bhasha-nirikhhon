import React, { useState } from 'react'
import { Redirect } from 'react-router-dom'
import axios from 'axios'

import apiUrl from '../../apiConfig'
import ReviewForm from '../shared/ReviewForm'
import Layout from '../shared/Layout'

const ReviewCreate = (props) => {
  const [review, setReview] = useState({ title: '', body: '', rating: '', show_id: '' })
  const [createdReviewId, setCreatedReviewId] = useState(null)

  const handleChange = event => {
    event.persist()
    setReview(review => ({ ...review, [event.target.name]: event.target.value }))
  }

  const handleSubmit = event => {
    event.preventDefault()

    axios({
      url: `${apiUrl}/reviews`,
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${props.user.token}`
      },
      data: { review }
    })
      .then(res => setCreatedReviewId(res.data.review.id))
      .catch(console.error)
  }

  if (createdReviewId) {
    return <Redirect to={`/shows/${createdReviewId}`} />
  }

  return (
    <Layout>
      <ReviewForm
        review={review}
        handleChange={handleChange}
        handleSubmit={handleSubmit}
        cancelPath="/shows"
      />
    </Layout>
  )
}

export default ReviewCreate