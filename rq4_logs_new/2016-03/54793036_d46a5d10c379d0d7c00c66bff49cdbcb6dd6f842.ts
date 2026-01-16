import {register} from 'platypus';
import BaseViewControl from '../base/base.vc';
import RedditListerRepository from '../../repositories/redditlister/redditlister.repo';

export default class SinglePostViewControl extends BaseViewControl {
    templateString: string = require('./singlepost.vc.html');
    
    constructor(private redRepo: RedditListerRepository) {
        super();
    }

    context: any = {
        
    };
    
    navigatedTo(parameters: { id: string } ): void {
        let id = parameters.id;
        this.context = this.redRepo.getOneReddit(id);
    }
     
}



register.viewControl('singlepost-vc', SinglePostViewControl, [RedditListerRepository]);