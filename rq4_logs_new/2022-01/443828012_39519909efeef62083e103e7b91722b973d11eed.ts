// import Board from './Board';
let BgReact = require('boardgame.io/react');

const createGame = () => {
  return {
    name: 'Capsaicin',
    setup: () => {
      return ({

      });
    },
    moves: {

    },
    endIf: (G: any, ctx: any) => {
        
    }
  };
}

const createClient = () => {
  return BgReact.Client({
    game: createGame(),
    numPlayers: 1,
    // board: Board,
    debug: true,
  });
}

export default createClient;