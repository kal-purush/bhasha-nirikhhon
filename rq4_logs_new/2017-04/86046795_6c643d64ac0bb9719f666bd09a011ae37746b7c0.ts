import * as restify from 'restify'
import * as mongoose from 'mongoose'
import IController from './controller'
import { Post, IPostDocument, IPostImage, PostStatus, PostType } from '../models/post.model'
import { User, IUserDocument, UserRole } from '../models/user.model'
import { logger } from '../../utils/logger'
import * as md5 from 'md5'
import HttpStatus from '../../utils/httpStatus'
import UserData from '../dataContract/data.user'
import AuthorData from '../dataContract/data.author'
import PostData from '../dataContract/data.post'
import PostImage from '../dataContract/data.postImage'
import { config } from '../../config/env'
import loginRequired from '../../utils/decorators/loginRequired'

export default class PostController implements IController {
    /**
     * 搜索一篇Post, 并附加到req.params
     * @param req
     * @param res
     * @param next
     * @returns {IPostDocument}
     */
    public load (req: restify.Request, res: restify.Response, next: restify.Next) {
        Post.findById(req.params.id)
        .then((post: IPostDocument) => {
            req.params.post = post
            return next()
        })
        .catch((err: any) => next(err))
    }

    /**
     * 获取一篇Post
     * @param req
     * @param res
     * @param next
     * @returns {PostData}
     */
    public get (req: restify.Request, res: restify.Response, next: restify.Next) {
        const post: IPostDocument = req.params.post
        const author: IUserDocument = User.findById(post._id)
        let authorData = new AuthorData()
        authorData._id = author._id
        authorData.name = author.nickname
        authorData.url = config.clientUrl + '/u/' + author._id
        authorData.avatar = author.avatar

        let postData = new PostData()
        postData._id = post._id
        postData.title = post.title
        postData.content = post.content
        postData.url = config.clientUrl + '/p/' + post._id
        postData.createdAt = post.createdAt
        postData.updatedAt = post.updatedAt
        postData.status = PostStatus[post.status]
        postData.type = PostType[post.type]
        postData.author = authorData
        postData.images = post.images
        postData.featuredImage = post.featuredImage
        postData.comments = post.comments
        postData.views = post.views
        postData.likes = post.likes

        res.json(HttpStatus.OK, postData)
        return next()
    }

    /**
     * 创建一篇Post并返回
     * @param req
     * @param res
     * @param next
     * @property {string} req.params.title - Post标题
     * @property {string} req.params.content - Post内容
     * @property {string} req.params.password - The password
     * @returns {IPostDocument}
     */
    @loginRequired
    public create (req: restify.Request, res: restify.Response, next: restify.Next) {
        const user: IUserDocument = req.params.user
        const allowRoles = [UserRole.admin, UserRole.editor, UserRole.author, UserRole.contributor]
        if (allowRoles.indexOf(user.role) < 0) {
            return next(new restify.ForbiddenError('你没有权限发布Post'))
        }
        const status = req.params.status
        if (status === 'publish' && user.role === UserRole.contributor) {
            // 投稿用户Post必须先pending
            status === 'pending'
        }

        const images = req.params.images
        if (!(images instanceof Array) || images.length < 1) {
            return next(new restify.InvalidArgumentError('图片数据不合法'))
        }

        let postImages = images.map((image: any) => {
            return new PostImage(image.url, image.originUrl, image.title, image.description)
        })

        const featuredImage = req.params.featuredImage
        let postFeaturedImage = featuredImage ? featuredImage as PostImage : postImages[0]

        let post: IPostDocument = new Post({
            title: req.params.title,
            content: req.params.content,
            createdAt: new Date(),
            status: PostStatus[status] === undefined ? PostStatus.pending : PostStatus[status],
            type: PostType[req.params.type] !== undefined ? PostType[req.params.type] : PostType.gallery,
            author: user._id,
            images: postImages,
            featuredImage: postFeaturedImage
        })

        return post
        .save()
        .then((savedPost: IPostDocument) => {
            res.json(HttpStatus.CREATED, savedPost)
            return next()
        })
        .catch((err: any) => next(err))
    }

    public update (req: restify.Request, res: restify.Response, next: restify.Next) {

    }

    public remove (req: restify.Request, res: restify.Response, next: restify.Next) {

    }
}