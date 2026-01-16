
// ./data/feed.js

// TODO: replace placholder data with database queries

export function fetchActivities() {
    // Sample data for the feed
    return [
      {
        id: 1,
        user: 4,
        content: 'Checkout <a href="#">this article</a> about the challenges to Web3 adoption',
        link: 'https://example.com',
        target: {
          name: 'Web3',
          link: '#'
        },
        type: 'comment',
        taxonomy: 'Community'
      },
      {
        id: 2,
        user: 2,
        content: 'What a good design! I like how you dealt with the spacing. Where can I get this file?',
        target: {
          name: 'Sora UI Kit',
          link: '#'
        },
        type: 'comment',
        taxonomy: 'Personal'
      },
      {
        id: 3,
        user: 3,
        content: '<a href="#">@Benjamin</a> here is the link <a href="#">supaui.com/download</a>',
        type: 'reply',
        taxonomy: 'Personal'
      },
      {
        id: 4,
        user: 4,
        content: 'generated new images on MidJourney',
        image: '/images/headphones.png',
        time: '8h ago',
        type: 'image-post',
        taxonomy: 'Personal'
      },
      {
        id: 5,
        user: 5,
        content: 'Just finished a marathon coding session, check out my latest commit!',
        link: 'https://github.com/andymagill',
        target: {
          name: 'Github',
          link: 'https://github.com'
        },
        type: 'comment',
        taxonomy: 'Team'
      },
      {
        id: 6,
        user: 0,
        content: 'generated new images on <strong>Stable Diffusion</strong>',
        image: '/images/headphones2.png',
        time: '8h ago',
        type: 'image-post',
        taxonomy: 'Community'
      },
      {
        id: 7,
        user: 3,
        content: 'Working my first blog post on decentralized finance!',
        link: '#',
        target: {
          name: 'DeFi',
          link: '#'
        },
        type: 'comment',
        taxonomy: 'Team'
      }
    ];
}


export function fetchUsers() {

  return [
    {
      id: 0,
      name: 'Jacob',
      avatar: '/images/jacob.svg'
    },
    {
      id: 1,
      name: 'Benjamin',
      avatar: '/images/benjamin.svg'
    },
    {
      id: 2,
      name: 'Alex',
      avatar: '/images/alex.svg'
    },
    {
      id: 3,
      name: 'Jacob',
      avatar: '/images/jacob.svg'
    },
    {
      id: 4,
      name: 'Mia',
      avatar: '/images/mia.svg'
    },
    {
      id: 5,
      name: 'Carlos',
      avatar: '/images/Carlos.svg'
    }
  ];
}

export function fetchReplies() {
  // Sample data for activity replies
  return [
    {
      id: 1,
      activityId: 1,
      user: {
        name: 'Alex',
        avatar: '/images/alex.svg'
      },
      content: '<a href="#">@Benjamin</a> here is the link <a href="#">supaui.com/download</a>',
      type: 'reply'
    },
    {
      id: 2,
      activityId: 7,
      user: {
        name: 'Mia',
        avatar: '/images/mia.svg'
      },
      content: 'Well done <a href="#">@Alex</a>!',
      type: 'reply'
    }
  ];
}

export function fetchLikes() {
  // Sample data for activity likes
  return [
    {
      id: 1,
      activityId: 1,
      userId: 1
    },
    {
      id: 2,
      activityId: 1,
      userId: 2
    },
    {
      id: 3,
      activityId: 1,
      userId: 3
    },
    {
      id: 4,
      activityId: 1,
      userId: 4
    },
    {
      id: 5,
      activityId: 1,
      userId: 5
    }
  ];
}
