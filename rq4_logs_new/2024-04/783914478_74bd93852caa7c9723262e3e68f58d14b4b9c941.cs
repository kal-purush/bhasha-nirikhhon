using LogicLayer;
using LogicLayer.Models;
using UnitTests.MockData;

namespace UnitTests;

public class CoreTests
{
    private readonly List<Player> _players = 
    [
        new Player
        {
            Id = 1,
            Username = "Player 1",
            Email = "Test1@mail.com",
            Password = "passwordTest1",
            Lives = 3
        },

        new Player
        {
            Id = 2,
            Username = "Player 2",
            Email = "Test2@mail.com",
            Password = "passwordTest2",
            Lives = 1
        },

        new Player
        {
            Id = 3,
            Username = "Player 3",
            Email = "Test3@mail.com",
            Password = "passwordTest3",
            Lives = 0
        },

        new Player
        {
            Id = 4,
            Username = "Player 4",
            Email = "Test4@mail.com",
            Password = "passwordTest4",
            Lives = 2
        },

        new Player
        {
            Id = 5,
            Username = "Player 5",
            Email = "Test5@mail.com",
            Password = "passwordTest5",
            Lives = 5
        }
    ];
    
    
    [SetUp]
    public void Setup()
    {
        var playerService = new MockDataService<Player>(_players);
        
        Core.Init(playerService);
    }
    
    

    [Test]
    public void GetFromKeyTest()
    {
        var player = Core.GetFromKey(new Player(0, "Test3@mail.com"));
        
        
        if (player == null)
        {
            Assert.Fail("Player not found");
            return;
        }
        
        
        if (player.Id != 3) Assert.Fail("Player Id is not 3");
        if (player.Username != "Player 3") Assert.Fail("Player Username is not Player 3");
        
        Assert.Pass();
        
    }
}















