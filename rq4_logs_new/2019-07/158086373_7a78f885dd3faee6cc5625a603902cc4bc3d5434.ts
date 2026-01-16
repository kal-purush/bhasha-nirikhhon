import { ModelBlogPost } from "./src/model/blog/post";
import { ModelUserUser } from "./src/model/user/user";
import { ModelUserRole } from "./src/model/user/role";
import { DefaultNamingStrategy } from "typeorm";
declare module "rapin" {
  interface Context {
    model_blog_post: ModelBlogPost;
    model_user_user: ModelUserUser;
    model_user_role: ModelUserRole;
  }
}