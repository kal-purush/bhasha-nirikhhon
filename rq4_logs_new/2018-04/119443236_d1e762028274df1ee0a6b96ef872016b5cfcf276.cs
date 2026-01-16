using System.Linq;
using UnityEngine;
using UnityStandardAssets.Characters.FirstPerson;

public class GameController : MonoBehaviour
{
    private static GameController _isntance;
    public static GameController Instance
    {
       get
        {
            if (_isntance == null)
            {
                object newObject = new object();

                lock (newObject)
                {
                    if (_isntance == null)
                    {
                        GameController[] instances = FindObjectsOfType<GameController>();

                        if (instances.Length > 0)
                            _isntance = instances[0];
                        else
                        {
                            GameObject newGameObject = new GameObject("GameController");
                            _isntance = newGameObject.AddComponent<GameController>();
                        }

                        instances.ToList().ForEach(p => { if (p != _isntance) Destroy(p.gameObject); });
                    }
                }
            }

            return _isntance;
        }
    }

    private static GameObject _player;
    public static GameObject Player
    {
        get
        {
            if (_player == null)
            {
                _player = FindObjectOfType<FirstPersonController>().gameObject;

                if (_player == null)
                    throw new System.Exception("No player found!");
            }

            return _player;
        }
    }
}