import {ChatInfo, MessageData, Updates} from "../../assets/Types";

export class ValidMockChatRepository {
    private database: any;
    private chats_ref: any;
    private memberships_ref: any;
    private messages_ref: any;

    constructor() {
        this.database = null;
        this.chats_ref = null;
        this.messages_ref = null;
        this.memberships_ref = null;
    }

    useRefs(){
        let test: any;
        test['one'] = this.chats_ref
        test['two'] = this.database
        test['three'] = this.memberships_ref
        test['four'] = this.messages_ref
        return test
    }

    async addMessage(chatId: string, message: MessageData) {
        console.log('entered Mock addMessage')
        return "message added"
    }

    async removeMessages(chatId: string) {
        console.log('entered Mock removeMessages')
        return "done"
    }

    async updateChatInfo(chatId: string, data: Updates | null) {
        console.log('entered Mock updateChatInfo')
        return "okay"
    }

    async getChatInfo(chatId: string): Promise<ChatInfo> {
        console.log('entered Mock getChatInfo')
        return Promise.resolve({
            _id: 'chatId',
            createdAt: 1,
            members: {
                id1: true
            },
            flatId: 'test',
            userId: 'test',
            title: {
                forFlat: 'test',
                forUser: 'test'
            }

        })
    }

    async getMembers(chatId: string): Promise<string[]> {
        console.log('entered Mock getMembers')
        return Promise.resolve([
            'user1',
            'user2'
        ])
    }

    async removeMemberships(chatId: any, members: string[]) {
        console.log('entered Mock removeMemberships')
        return 'done';
    }

    async addMemberships(chatId: string, profileId: string, members: string[]) {
        console.log('entered Mock addMemberships')
        return 'done'
    }

    async getMemberships(userId: string): Promise<string[] | null> {
        console.log('entered Mock getMemberships')
        return Promise.resolve([
            'chatId1',
            'chatId2'
        ])
    }

    async getMembershipValues(userId: string): Promise<string[] | null> {
        console.log('entered Mock getMembershipValues')
        if(userId.includes('hasChat')){
            return Promise.resolve([
                'profile1',
                'flt$match'
            ])
        }
        return Promise.resolve([
            'profile1',
            'flt$test'
        ])
    }


}