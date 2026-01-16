using UnityEngine;
using UnityEngine.InputSystem;

public abstract class NPC : MonoBehaviour, IInteractable
{
    [SerializeField]
    private SpriteRenderer _interactSprite;
    private Transform _playerTransform;
    private const float INTERACT_DISTANCE = 5f;
    private void Start()
    {
        _playerTransform = GameObject.FindGameObjectWithTag("Player").transform;
    }
    private void Update()
    {
        if (Keyboard.current.eKey.wasPressedThisFrame && IsWithinInteractDistance())
        {
            //interface
            Interact();
        }
        if(_interactSprite.gameObject.activeSelf && !IsWithinInteractDistance())
        {
            //turn off the sprite
            _interactSprite.gameObject.SetActive(false);
        }
        else if (! _interactSprite.gameObject.activeSelf && IsWithinInteractDistance())
        {
            //turn on the sprite
            _interactSprite.gameObject.SetActive(true);
        }
    }

    public abstract void Interact();

    private bool IsWithinInteractDistance()
    {
        if (Vector2.Distance(_playerTransform.position, transform.position) < INTERACT_DISTANCE)
        {
            return true;
        }
        else
        {
            return false;
        }
    }


}