/* tslint:disable */

class _RedditClient {
    public getPost(postId: number) {
        return fetch("http://localhost/post/" + postId);
    }
}

export const RedditClient = new _RedditClient();