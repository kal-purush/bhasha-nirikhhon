import React, { useState, useEffect } from 'react'
import Container from '@material-ui/core/Container'
import Box from '@material-ui/core/Box'
import moment from 'moment'

import PageTitle from '../PageTitle'
import { withFirebase } from '../Firebase'



const DailyLeaders = ({ match, db }) => {
  const stateCode = match.params.stateCode.toUpperCase()
  const [post, setPost] = useState()

  useEffect(() => {
    (async () => {
      const snap = await db.collection(`/states/${stateCode}/posts/`).orderBy('dateID', 'desc').limit(1).get()
      console.log('getting leader data')
      setPost(snap.docs[0].data())
    })()
  }, [db, stateCode])
  const src = (leader) => {
    return `https://firebasestorage.googleapis.com/v0/b/repsp123-leaders/o/${leader.PhotoFile}?alt=media`
  }
  if (!post) return null

  return (
    <Container maxWidth="md">
      <Box my={2}>
        <div>
          <PageTitle stateCode={stateCode} />
        </div> 
      </Box>
      <Box my={2}> 
          {post.dateID &&
            <div>
              <h3>{moment(post.dateID).format('dddd, MMMM Do, YYYY')}</h3>
              <h2>Today we are praying for</h2>
            </div>
          }
      </Box>
      <Box my={2}> 
            <img src={src(post.leader1)} alt="Leader"/>
            <img src={src(post.leader2)} alt="Leader"/>
            <img src={src(post.leader3)} alt="Leader"/>
      </Box>
      <Box my={2}> 
            <h3>{post.leader1.Title} {post.leader1.FirstName} {post.leader1.LastName}</h3>
            <h3>{post.leader2.Title} {post.leader2.FirstName} {post.leader2.LastName}</h3>
            <h3>{post.leader3.Title} {post.leader3.FirstName} {post.leader3.LastName}</h3>
      </Box> 
      </Container>
  )
}

export default withFirebase(DailyLeaders)