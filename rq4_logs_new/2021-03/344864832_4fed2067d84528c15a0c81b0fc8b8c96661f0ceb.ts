import {FollowerFollow, FollowerFollowModel} from '../models/FollowerFollow';

class FollowerFollowService {
    findFollow(id: string){
        return FollowerFollow.find({
            follower: id
        })
    }

    follow(folllowFollower: FollowerFollowModel){
        return FollowerFollow.create(folllowFollower);
    }
    
    unfollow(followerId: string, followId: string) {
        return FollowerFollow.deleteOne({
            follow: followId,
            follower: followerId
        })
    }
}

const followerFollowService = new FollowerFollowService();
export {
    followerFollowService
}