import {async, register} from 'platypus';
import BaseRepository from '../base/base.repo';
import APIService from '../../services/api/api.svc';

export default class PostsRepository extends BaseRepository {
    posts: any = [];
    test: any = "works";
    
    constructor(private apiSvc: APIService) {
        super();
        //console.log("repository active");
        //console.log(apiSvc.serviceTest());
        
        
    }
    
    populatePosts() {
        //let response = this.apiSvc.getAllPosts();
        //console.log(response);
        //this.posts = response;
        
        return this.apiSvc.getAllPosts().then((response) => {
            //console.log(response);
            this.posts = response;
            return response;
        });
    }
    

}

register.injectable('posts-repo', PostsRepository, [APIService]);