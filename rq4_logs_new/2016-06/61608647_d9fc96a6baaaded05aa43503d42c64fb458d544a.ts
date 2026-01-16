import {Comment} from '../model/comment';

export class InMemoryMockDataService {

  public comments: Comment[] = [
    {
      id: 1, name: 'Mr. Nice', date: 'November 5, 1955',
      content: 'Wait a minute. Wait a minute, Doc. Uhhh... Are you telling me that you built a time machine... out of a DeLorean?! Whoa. This is heavy.',
      likes: 6, ammountComments: 12, lastUpdate: '6 days', avatarUrl: 'https://d3rt1990lpmkn.cloudfront.net/unbranded/9bdd59379eb0e0815301c00fb985b6549f5221eb',
      imageUrl: 'https://img.stb.s-msn.com/usappex/tenant/amp/entityid/BBfcpPq_m4_h400_w600.png'
    },
    {
      id: 1, name: 'John Smith', date: 'Sep 5, 1986',
      content: 'Your scientists were so preoccupied with whether or not they could, that they didn\'t stop to think if they should.',
      likes: 65, ammountComments: 1322, lastUpdate: '6 days', avatarUrl: 'https://d3rt1990lpmkn.cloudfront.net/unbranded/37f615e5ec794b796556f99c608b6e283dc27286',
      imageUrl: 'https://img.stb.s-msn.com/usappex/tenant/amp/entityid/AA9yPlh_m4_h400_w600.jpg'
    },
    {
      id: 1, name: 'Christ', date: 'November 4, 1993',
      content: 'Hello, World!',
      likes: 545, ammountComments: 1322, lastUpdate: '6 days', avatarUrl: 'https://d3rt1990lpmkn.cloudfront.net/unbranded/c0a1734e77ce741761bee6a4f788865a48ba92b6',
      imageUrl: 'https://img.stb.s-msn.com/usappex/tenant/amp/entityid/AA9ySKy_m4_h400_w600.jpg'
    },
    {
      id: 1, name: 'Mr. Nice', date: 'November 5, 1955',
      content: "Do you struggle to build your chest? Then you've come to the right place. Sometimes your pecs just need a special type of TLC in the form of heavy weights, moderate weights, and challenging body weight exercises, which is exactly what this program provides.",
      likes: 6, ammountComments: 12, lastUpdate: '6 days', avatarUrl: 'https://d3rt1990lpmkn.cloudfront.net/unbranded/37f615e5ec794b796556f99c608b6e283dc27286',
      imageUrl: 'https://img.stb.s-msn.com/usappex/tenant/amp/entityid/BBfckaV_m4_h400_w600.jpg'
    }
  ];

  constructor() {

  }



}