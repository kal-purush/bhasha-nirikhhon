using UnityEngine;

/**
 * @todo restore quacks
 */
public class GameStartEvent: IEvent
{
    const int QUACK_MIN = 1;
    const int QUACK_MAX = 3;

    private AudioSource _music;
    private AudioSource _quack;
    private GameManager _game;
    private System.Random _quackInterval;


    public GameStartEvent(AudioSource music, AudioSource quack, GameManager game)
    {
        _music = music;
        _quack = quack;
        _game = game;
        _quackInterval = new System.Random();
    }

    public void Trigger()
    {
        _music.Play();
        //Invoke("Quack", _quackInterval.Next(QUACK_MIN, QUACK_MAX));
    }

    void Quack()
    {
        if (1 == _game.GetCurrentSceneIndex())
        {
            _quack.Play();
            //Invoke("Quack", _quackInterval.Next(QUACK_MIN, QUACK_MAX));
        }
    }
}