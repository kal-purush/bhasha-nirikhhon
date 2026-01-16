import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { Blog } from '../domain/blog.sql.entity';
import { TypeBlogHalper, TypePostForBlogHalper } from 'src/base/types/blog.types';
import { BlogViewModel, PaginatorBlogViewModel } from '../api/models/output.model';
import { PaginatorPostViewModel, PostViewModel } from '../../posts/api/models/output.model';
import { blogPagination } from 'src/base/models/blog.model';
import { likeStatus } from '../../likes/api/models/input.model';

@Injectable()
export class BlogQueryRepository {
    constructor(private dataSource: DataSource) {}

    async getAllBlogs(helper: TypeBlogHalper): Promise<PaginatorBlogViewModel> {
        const queryParams = blogPagination(helper);
        const search = helper.searchNameTerm
            ? `WHERE "name" ILIKE '%${helper.searchNameTerm}%'`
            : '';

        const query = `
            SELECT * FROM "Blogs"
            ${search}
            ORDER BY "${queryParams.sortBy}" ${queryParams.sortDirection}
            LIMIT ${queryParams.pageSize} OFFSET ${(queryParams.pageNumber - 1) * queryParams.pageSize}
        `;

        const items = await this.dataSource.query(query);
        const totalCount = await this.dataSource.query(`SELECT COUNT(*) FROM "Blogs" ${search}`);

        const blogs = {
            pagesCount: Math.ceil(totalCount[0].count / queryParams.pageSize),
            page: queryParams.pageNumber,
            pageSize: queryParams.pageSize,
            totalCount: parseInt(totalCount[0].count),
            items: items.map(this.blogMap),
        };

        return blogs;
    }

    async getBlogById(blogId: string): Promise<BlogViewModel | null> {
        const query = `SELECT * FROM "Blogs" WHERE id = $1`;
        const blog = await this.dataSource.query(query, [blogId]);

        if (!blog.length) {
            return null;
        }

        return this.blogMap(blog[0]);
    }

    async getPostForBlogById(postId: string): Promise<PostViewModel | null> {
        const query = `
            SELECT p.*, b."name" AS "blogName"
            FROM "Posts" AS p
            LEFT JOIN "Blogs" AS b 
                ON p."blogId" = b.id
            WHERE p.id = $1
        `;
        const post = await this.dataSource.query(query, [postId]);
    
        if (!post.length) {
            return null;
        }
        return this.mapPost(post[0]);
    }

    async getPostsForBlog(helper: TypePostForBlogHalper, id: string, userId: string | null): Promise<PaginatorPostViewModel> {
        const queryParams = blogPagination(helper);
    
        const query = `
        SELECT p.*, b."name" AS "blogName"
        FROM "Posts" AS p
        LEFT JOIN "Blogs" AS b 
            ON p."blogId" = b.id
        WHERE p."blogId" = $1
        ORDER BY "${queryParams.sortBy}" ${queryParams.sortDirection}
        LIMIT $2 OFFSET $3
        `;
    
        const posts = await this.dataSource.query(query, [id, queryParams.pageSize, (queryParams.pageNumber - 1) * queryParams.pageSize]);
        const totalCount = await this.dataSource.query(`SELECT COUNT(*) FROM "Posts" WHERE "blogId" = $1`, [id]);
    
        const items = await Promise.all(posts.map(async post => {
    
            return this.mapPost(post);
        }));
    
        return {
            pagesCount: Math.ceil(totalCount[0].count / queryParams.pageSize),
            page: queryParams.pageNumber,
            pageSize: queryParams.pageSize,
            totalCount: parseInt(totalCount[0].count),
            items,
        };
    }

    mapPost(post: any): PostViewModel {
        return {
            id: post.id,
            title: post.title,
            shortDescription: post.shortDescription,
            content: post.content,
            blogId: post.blogId,
            blogName: post.blogName,
            createdAt: post.createdAt,
            extendedLikesInfo: {
                likesCount: 0,
                dislikesCount: 0,
                myStatus: likeStatus.None,
                newestLikes: []
            },
        };
    }

    blogMap(blog: Blog): BlogViewModel {
        return {
            id: blog.id,
            name: blog.name,
            description: blog.description,
            websiteUrl: blog.websiteUrl,
            createdAt: blog.createdAt,
            isMembership: blog.isMembership,
        };
    }
}