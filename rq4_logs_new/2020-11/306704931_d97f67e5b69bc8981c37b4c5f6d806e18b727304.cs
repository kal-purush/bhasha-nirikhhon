using UnityEngine;
using UnityEngine.UI;

namespace JevLogin
{
    public sealed class CreateButton : MonoBehaviour
    {
        [SerializeField] private Button _button;
        [SerializeField] private Transform _root;

        private void Start()
        {
            try
            {
                if (_button == null)
                {
                    throw new MyException($"Геймдизайнер дурак! Забыл добавить кнопку на компонент {this.name}", _button);
                }
                for (int i = 0; i < 100; i++)
                {
                    var instantiate = Instantiate(_button, _root);
                    instantiate.GetComponentInChildren<Text>().text = i.ToString();
                    var temp = i;
                    instantiate.onClick.AddListener(() => Debug.Log(temp));
                }
            }
            catch (System.Exception e)
            {
                Debug.LogError($"{e.Message}");
            }
            
        }
    }
}