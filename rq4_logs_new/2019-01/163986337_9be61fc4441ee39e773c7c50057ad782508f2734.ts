import { PostResponse } from '../../../src/modules/content/components/postsList/postsList.model';

const postsListResponse: PostResponse = {
  "meta": {
    "total": 2, 
    "start": 0, 
    "end": 1
  },
  "content": [
    {"id": 1, "createdAt": "123", "title": "test_title_1", "tags": ["tag1"],"content": "test", "commentsCount": 2},
    {"id": 2, "createdAt": "1234", "title": "test_title_2", "tags": ["tag2", "tag3"], "content": "test", "commentsCount": 3},
    {"id": 2, "createdAt": "1234", "title": "test_title_2", "tags": ["tag2", "tag3"], "content": "test", "commentsCount": 3},
    {"id": 2, "createdAt": "1234", "title": "test_title_2", "tags": ["tag2", "tag3"], "content": "test", "commentsCount": 3},
    {"id": 2, "createdAt": "1234", "title": "test_title_2", "tags": ["tag2", "tag3"], "content": "test", "commentsCount": 3}
  ]
};

export default postsListResponse;