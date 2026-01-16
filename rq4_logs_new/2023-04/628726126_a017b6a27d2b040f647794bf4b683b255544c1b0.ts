import { FriendsService } from '../services/friends.service';
import { Body, Controller, Get, Post } from '@nestjs/common';
import { FriendsDto } from '../Dto/Friends.dto';

@Controller('friends')
export class FriendsController {
    constructor(private readonly friendsService: FriendsService) {}

    @Get()
    getFriends(): string {
        return this.friendsService.getFriends();
    }

    @Post()
    addFriend(@Body() friend: FriendsDto): Promise<any> {
        return Promise.resolve('Friend added');
    }
}