export default {
  type: "Scoreboard",
  states: {
    "OH": {
      classification: "Tossup",
      electoral_votes: 19,
      latest_poll: "July 12, 2016",
      poll_amount: 45,
      score: [ 41, 39, 20]
    },
    "PA": {
      classification: "Tossup",
      electoral_votes: 20,
      latest_poll: "July 12, 2016",
      poll_amount: 35,
      score: [43, 39, 17]
    }
  },
  _links: {
    self: { href: "/scoreboard"},
    candidates: [ { href: "/candidates/1" }, { href: "/candidates/2"} ]
  },
  _included: [ 
    {
      _links: { self: { href: '/candidates/1'} },
      name: "Hillary Clinton",
      party: "D",
      avatar: ""
    },
    {
      _links: { self: { href: '/candidates/2'} },
      name: "Donald Trump",
      party: "R",
      avatar: ""
    }
  ]
}