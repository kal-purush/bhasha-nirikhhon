
module.exports = {
    getComments(req, res) {
      res.status(200).send(req.store.posts[req.params.postId].comments)
    },
    
    addComment(req, res) {
      req.store.posts[req.params.postId].comments.push(req.body)
      res.status(201).send({ commentId: req.store.posts[req.params.postId].comments[req.store.posts[req.params.postId].comments.length - 1] })
    },

    updateComment(req, res) {
      Object.assign(req.store.posts[req.params.postId].comments[req.params.commentId], req.body)
      res.status(200).send(req.store.posts[req.params.postId].comments[req.params.commentId])  
    },

    removeComment(req, res) {
      req.store.posts[req.params.postId].comments.splice(req.params.commentId, 1)
      res.status(204).send()
    }  
}