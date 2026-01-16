using System;
using System.Linq;
using MineSweeper.Data.Models;
using MineSweeper.Generators;
using MineSweeper.Generators.Interfaces;
using MineSweeper.Models;

class GameSaveProvider : IGameSaveProvider
{
    private readonly IFieldGeneratorFactory FieldGeneratorFactory;

    public GameSaveProvider(IFieldGeneratorFactory fieldGeneratorFactory)
    {
        FieldGeneratorFactory = fieldGeneratorFactory;
    }

    public Game GetGameFromSave(GameSave gameSave)
    {
        if (gameSave == null) throw new ArgumentNullException(nameof(gameSave));

        IFieldGenerator generator = FieldGeneratorFactory.Create(gameSave.GeneratorParams);
        DateTime startTime = DateTime.UtcNow.Subtract(gameSave.Timer);

        var game = new Game(generator, gameSave.Width, gameSave.Height, startTime);

        foreach (var move in gameSave.PlayerMoves)
        {
            game.MakeMove(move);
        }

        return game;
    }

    public GameSave GetGameSave(Game game)
    {
        if (game == null) throw new ArgumentNullException(nameof(game));

        var gameSave = new GameSave
        {
            GeneratorParams = game.FieldGenerator.Parameters,
            PlayerMoves = game.PlayerMoves.ToList()
        };

        return gameSave;
    }
}