
class Battle
{
    public Monster Monster;
    public Player  Player;

    public Battle(Monster monsters, Player player)
    {
        this.Monster = monsters;
        this.Player = player;
    }

    public bool turn(string move)
    {
        return true;
    }
}