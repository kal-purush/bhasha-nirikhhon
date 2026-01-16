import * as restify from 'restify'
import * as mongoose from 'mongoose'
import IController from './controller'
import { Post, IPostDocument, IPostImage, PostStatus, PostType } from '../models/post.model'
import { Collection, ICollectionDocument } from '../models/collection.model'
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

export default class CollectionController implements IController {
    /**
     * 搜索单个Collection, 并附加到req.params
     * @param req
     * @param res
     * @param next
     * @returns {ICollectionDocument}
     */
    public load(req: restify.Request, res: restify.Response, next: restify.Next) {
        Collection.findByNewId(req.params.id)
            .then((collection: ICollectionDocument) => {
                req.params.collection = collection
                return next()
            })
            .catch((err: any) => next(err))
    }

    /**
     * 获取单个Collection
     * @param req
     * @param res
     * @param next
     * @returns {JSON}
     */
    public get(req: restify.Request, res: restify.Response, next: restify.Next) {
        const collection: ICollectionDocument = req.params.collection
        res.json(HttpStatus.OK, collection)
        return next()
    }

    /**
     * 创建一个Collection并返回
     * @param req
     * @param res
     * @param next
     * @property {string} req.params.name - Collection标题
     * @property {string} req.params.description - Collection描述
     * @property {string} req.params.post - Collection包含的post
     * @property {string} req.params.secret - 是否私有
     * @returns {ICollectionDocument}
     */
    @loginRequired
    public async create(req: restify.Request, res: restify.Response, next: restify.Next) {
        const user: IUserDocument = req.params.user
        const allowRoles = [UserRole.admin, UserRole.editor, UserRole.author, UserRole.contributor]
        if (allowRoles.indexOf(user.role) < 0) {
            return next(new restify.ForbiddenError('你没有权限创建收藏夹'))
        }

        const name = req.params.name
        const description = req.params.description
        const postId = req.params.post

        if (!name || name.length < 2) {
            return next(new restify.InvalidArgumentError('无效的收藏夹名称'))
        }

        // 检查是否已存在同名Collection
        try {
            await Collection.findByName(name)
            return next(new restify.InvalidArgumentError('指定的收藏夹名称已存在'))
        }
        catch (err) {

        }

        // 如果附带了post
        let post: IPostDocument = null
        let featuredImage: PostImage = null
        if (postId) {
            try {
                post = await Post.findByNewId(postId)
                featuredImage = post.featuredImage
            }
            catch (err) {

            }
        }

        let collection: ICollectionDocument = new Collection({
            name: name,
            description: description,
            slug: encodeURIComponent(name),
            author: user._id,
            createdAt: new Date(),
            secret: !!req.params.secret,
            posts: postId ? [postId] : [],
            featuredImage: featuredImage 
        })

        return collection
            .save()
            .then((savedCollection: ICollectionDocument) => {
                res.json(HttpStatus.CREATED, savedCollection)
                return next()
            })
            .catch((err: any) => next(err))
    }

    public update(req: restify.Request, res: restify.Response, next: restify.Next) {

    }

    public remove(req: restify.Request, res: restify.Response, next: restify.Next) {

    }
}