enum PostsTypes {
  Lost,
  Found,
}

export type PostType = {
  img: string;
  details: {
    animal: string;
    age: string;
    gender: string;
    color: string;
    createdBy: string;
    location: string;
    date: string;
    description: string;
    type: PostsTypes;
  };
  id: string;
  isLiked: boolean;
};

console.log(PostsTypes.Found);