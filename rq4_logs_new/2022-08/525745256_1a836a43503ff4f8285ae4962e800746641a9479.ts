import { Request, Response } from "express";
import { Game, gamesLib } from "./game.model";

export function getAllGames(req: Request, res: Response) {
  if (gamesLib.length <= 0) {
    res.status(404).json("The library is empty");
  } else {
    res.status(200).json(
      gamesLib.sort((g1, g2) => {
        if (g1.id < g2.id) return -1;
        if (g1.id > g2.id) return 1;
        return 0;
      })
    );
  }
}

export function addTestData(req: Request, res: Response) {
  if (gamesLib.length > 0) {
    res
      .status(403)
      .json(
        "The game library is not empty, therefore test data won't be added"
      );
  } else {
    for (let i = 0; i < 5; i++) {
      let game: Game = {
        id: `${i}`,
        title: `TomIsKing${i}`,
        releaseDate: new Date(),
        genre: "Action",
        rating: "M",
      };

      gamesLib.push(game);
    }

    res.status(200).json(gamesLib);
  }
}

export async function getGameById(req: Request, res: Response) {
  let game = await gamesLib.find((g) => g.id === req.params.id);

  if (game === undefined) {
    res.status(404).json("A game does not exist with that id");
  } else {
    res.status(200).json(game);
  }
}

export function addNewGame(req: Request, res: Response) {
  // TODO: Lägga till validering
  let idOfNewGame = new Date().getMilliseconds();

  let newGame: Game = {
    id: idOfNewGame.toString(),
    title: `${req.body.title}`,
    releaseDate: req.body.releaseDate,
    genre: `${req.body.genre}`,
    rating: `${req.body.rating}`,
  };

  gamesLib.push(newGame);

  res.status(201).json(newGame);
}

export function changeGameInfo(req: Request, res: Response) {
  if (gamesLib.some((g) => g.id === req.params.id)) {
    gamesLib.forEach((g) => {
      if (g.id === req.params.id) {
        g.title = req.body.title;
        g.releaseDate = req.body.releaseDate;
        g.genre = req.body.genre;
        g.rating = req.body.rating;
        res.status(200).json(g);
      }
    });
  } else {
    res.status(404).json("A game does not exist with that id");
  }
}

export function deleteGameById(req: Request, res: Response) {
  let gameFoundAndDeleted: boolean = false;

  gamesLib.forEach((g) => {
    if (g.id === req.params.id) {
      gamesLib.splice(gamesLib.indexOf(g), 1);

      gameFoundAndDeleted = true;
    }
  });

  if (!gameFoundAndDeleted)
    res.status(404).json("A game does not exist with that id");
  else res.status(204).json(null);
}