using LogicLayer.Interfaces;
using LogicLayer.Models;

namespace LogicLayer;

public static class Core
{

    private static IDatabaseEntityService<Player> _playerService = null!;
    
    public static void Init(IDatabaseEntityService<Player> playerService)
    {
        _playerService = playerService;
    }

    
    
    public static List<Player> GetAllPlayers()
    {
        return _playerService.GetAll().GetAwaiter().GetResult()!;
    }
    
    public static Player GetFromKey(Player player)
    {
        return _playerService.GetFromKey(player).GetAwaiter().GetResult()!;
    }
    
    
    
    
}