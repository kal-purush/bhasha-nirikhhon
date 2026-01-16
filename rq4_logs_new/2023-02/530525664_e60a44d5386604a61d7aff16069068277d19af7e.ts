import { UserStatus } from '../xplat/types/common';
import { User, QueryCursor, Post, Route, Forum, RouteStatus } from '../xplat/types/types';
import { INITIAL_CURSOR_SIZE } from './constants';
import { getRouteById } from '../xplat/api/route';

type FetchedUser = {
    username: string;
    email: string;
    displayName: string;
    bio: string;
    status: UserStatus;
    avatarUrl: string;
    userObject: User;
};
export const buildUserFetcher = (user: User) => {
    return async (): Promise<FetchedUser> => {
        await user.getData();
        return {
            username: await user.getUsername(),
            email: await user.getEmail(),
            displayName: await user.getDisplayName(),
            bio: await user.getBio(),
            status: await user.getStatus(),
            avatarUrl: await user.getAvatarUrl(),
            userObject: user,
        } as FetchedUser;
    };
};

type FetchedPost = {
    body: string;
    author: User;
    timestamp: Date;
    likes: number;
    imageURLs: string[];
    imageCount: number;
    videoContent:
    | {
        videoUrl: string;
        thumbnailUrl: string;
      }
    | undefined;
    postObject: Post;
};
export const buildPostFetcher = (post: Post) => {
    return async (): Promise<FetchedPost> => {
        await post.getData();
        return {
            body: await post.getTextContent(),
            author: await post.getAuthor(),
            timestamp: await post.getTimestamp(),
            imageCount: await post.getImageCount(),
            imageURLs: await post.getImageContentUrls(),
            likes: await (await post.getLikes()).length,
            videoContent: (await post.hasVideoContent())
                ? {
                    videoUrl: await post.getVideoUrl(),
                    thumbnailUrl: await post.getVideoThumbnailUrl(),
                }
                : undefined,
            postObject: post,
        } as FetchedPost;
    };
};

type FetchedForum = {
    postCursor: QueryCursor<Post>;
    posts: Post[];
    hasMore: boolean;
};
export const buildForumFetcher = (forum: Forum) => {
    return async (): Promise<FetchedForum> => {
        const postCursor = forum.getPostsCursor();
        const tempPosts: Post[] = [];
        let hasNext = await postCursor?.hasNext();
        while((hasNext) && tempPosts.length < INITIAL_CURSOR_SIZE){
            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            tempPosts.push((await postCursor?.pollNext())!);
            hasNext = await postCursor?.hasNext();
        }
            
        return {
            postCursor: postCursor,
            posts: tempPosts,
            hasMore: hasNext,
        } as FetchedForum;
    };
};

type FetchedRoute = {
    name: string;
    setter: {
        raw: boolean;
        string: string;
    }
    forum: Forum;
    description: string;
    archived: RouteStatus;
};
export const buildRouteFetcher = (route: Route) => {
    return async (): Promise<FetchedRoute> => {
        await route.getData();
        return {
            name: await route.getName(),
            setter: {
                raw: await route.getSetter() === undefined,
                string: (await route.getSetter() === undefined)
                    ? 
                    route.getSetterRawName() 
                    : 
                    await (await route.getSetter()).getDisplayName(),
            },
            forum: await route.getForum(),
            description: await route.getDescription(),
            archived: await route.getStatus(),
        } as FetchedRoute;
    };
};
export const buildRouteFetcherByID = (routeID: string) => {
    return async (): Promise<FetchedRoute> => {
        const route = await getRouteById(routeID);
        await route.getData().catch( (invalidUID) => {
            return Promise.reject(invalidUID);
        });
        return buildRouteFetcher(route)();
    };
};