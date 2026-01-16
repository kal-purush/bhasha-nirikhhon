const initialState = {
  bets: [
    {
      avatar: "/static/danielle.png",
      stake: 10,
      resolved: false,
      name: "Danielle",
      market: "Winner",
      prediction: "Manchester United",
      startTime: 0,
      performance: {
        time: 10000,
        result: true,
      }
    },
    {
      avatar: "/static/mike.png",
      stake: 5,
      payout: 15,
      resolved: false,
      name: "Mike",
      startTime: 0,
      market: "Next Goal Scorer",
      prediction: "Ronaldo",
      performance: {
        time: 45,
        result: true,
      }
    },
    {
      avatar: "/static/roland.png",
      stake: 10,
      resolved: false,
      name: "Roland",
      startTime: 30,
      market: "Next Goal Scorer",
      prediction: "Rooney",
      performance: {
        time: 45,
        result: false,
      }
    },
    {
      avatar: "/static/ben.png",
      stake: 15,
      payout: 45,
      resolved: false,
      name: "Ben",
      startTime: 35,
      market: "Next Goal Scorer",
      prediction: "Ronaldo",
      performance: {
        time: 45,
        result: true,
      }
    },
  ]
};

const flatMap = (arr, fn) => {
  return arr.reduce((acc, el) => [...acc, ...fn(el)], []);
};

export default (state = initialState, action) => {
    switch(action.type) {
      case 'SET_TIME':
        const bets = flatMap(state.bets, bet => {
          if(bet.performance.time < (action.time - 5)) {
            return [{ ...bet, finished: true }];
          } else if(!bet.resolved && action.time > bet.performance.time) {
            return [{ ...bet, resolved: true, result: bet.performance.result }];
          } else {
            return [bet];
          }
        });

        return { ...state, time: action.time, bets };
      default:
          return state;
    }
};