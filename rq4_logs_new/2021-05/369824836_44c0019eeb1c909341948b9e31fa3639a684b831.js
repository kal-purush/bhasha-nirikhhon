import mongoose from 'mongoose'

const postSchema = mongoose.Schema({
    title: String,
    body: String,
    author: String
})

export default mongoose.model('posts',postSchema)