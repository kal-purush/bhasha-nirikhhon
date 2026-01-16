export class InMemoryMockDataService {
  createDb() {
    let commentsMock = [
      {id: 1, name: 'Mr. Nice', date: 'November 5, 1955', 
      content: 'Wait a minute. Wait a minute, Doc. Uhhh... Are you telling me that you built a time machine... out of a DeLorean?! Whoa. This is heavy.',
    likes: 6, ammountComments: 12, lastUpdate: '6 days', avatarUrl: 'https://d3rt1990lpmkn.cloudfront.net/unbranded/37f615e5ec794b796556f99c608b6e283dc27286',
    imageUrl: 'https://i.scdn.co/image/b48ce972e59663fde694492f4aa5dcf36e7110de'},
          {id: 1, name: 'Mr. Nice', date: 'November 5, 1955', 
      content: 'Your scientists were so preoccupied with whether or not they could, that they didn\'t stop to think if they should.',
    likes: 6, ammountComments: 12, lastUpdate: '6 days', avatarUrl: 'https://d3rt1990lpmkn.cloudfront.net/unbranded/37f615e5ec794b796556f99c608b6e283dc27286',
    imageUrl: 'https://i.scdn.co/image/b48ce972e59663fde694492f4aa5dcf36e7110de'},
         {id: 1, name: 'Mr. Nice', date: 'November 5, 1955', 
      content: 'Wait a minute. Wait a minute, Doc. Uhhh... Are you telling me that you built a time machine... out of a DeLorean?! Whoa. This is heavy.',
    likes: 6, ammountComments: 12, lastUpdate: '6 days', avatarUrl: 'https://d3rt1990lpmkn.cloudfront.net/unbranded/37f615e5ec794b796556f99c608b6e283dc27286',
    imageUrl: 'https://i.scdn.co/image/b48ce972e59663fde694492f4aa5dcf36e7110de'},
          {id: 1, name: 'Mr. Nice', date: 'November 5, 1955', 
      content: 'Wait a minute. Wait a minute, Doc. Uhhh... Are you telling me that you built a time machine... out of a DeLorean?! Whoa. This is heavy.',
    likes: 6, ammountComments: 12, lastUpdate: '6 days', avatarUrl: 'https://d3rt1990lpmkn.cloudfront.net/unbranded/37f615e5ec794b796556f99c608b6e283dc27286',
    imageUrl: 'https://i.scdn.co/image/b48ce972e59663fde694492f4aa5dcf36e7110de'}
    ];

    return {commentsMock};
  }
}