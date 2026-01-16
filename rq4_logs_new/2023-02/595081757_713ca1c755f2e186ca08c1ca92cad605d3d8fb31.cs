using System.Collections.Generic;
using Events;
using UnityEngine;
using UnityEngine.UI;

namespace UI
{
    public class PopupWindow : MonoBehaviour
    {
        [SerializeField] private List<GameObject> popupObjects;
        [SerializeField] private List<Image> itemSprites;
        [SerializeField] private List<Sprite> itemLargeSprites;

        [SerializeField] private Image largeImage;

        private List<Outline> _itemOutlines = new List<Outline>();
        private int _selectIndex = 0;

        [SerializeField] private bool isOpen = false;

        private float _targetSize = 0f;
        private float _largeTargetSize = 0f;

        [SerializeField] [Header("Popup Item")]
        private bool popupHasItem;

        [SerializeField] private int itemPosition;
        [SerializeField] private Sprite substituteSprite;
        [SerializeField] private GameObject popupItem;
        [SerializeField] private DefaultEvent eventToTrigger;
        private bool _hasGottenItem = false;

        private void Awake()
        {
            itemPosition--;

            foreach (GameObject item in popupObjects)
            {
                _itemOutlines.Add(item.GetComponent<Outline>());
            }

            ResetSpriteColor();
        }

        public void OpenClosePopup()
        {
            switch (isOpen)
            {
                case true:
                    _targetSize = 0f;
                    break;

                case false:
                    _targetSize = 1f;
                    ResetSelection();
                    break;
            }

            isOpen = !isOpen;
        }

        private void Update()
        {
            transform.localScale = Vector3.Lerp(transform.localScale,
                new Vector3(_targetSize, _targetSize, _targetSize), Time.deltaTime * 5f);
            largeImage.transform.localScale = Vector3.Lerp(largeImage.transform.localScale,
                new Vector3(_largeTargetSize, _largeTargetSize, _largeTargetSize), Time.deltaTime * 12.5f);

            if (Input.GetKeyDown(KeyCode.LeftArrow) && isOpen)
            {
                SelectPrevious();
            }

            if (Input.GetKeyDown(KeyCode.RightArrow) && isOpen)
            {
                SelectNext();
            }

            if (Input.GetKeyDown(KeyCode.UpArrow) && isOpen)
            {
                OpenBook();
            }
        }

        private void OpenBook()
        {
            _largeTargetSize = 1f;
            largeImage.sprite = itemLargeSprites[_selectIndex];
            largeImage.gameObject.SetActive(true);

            if (popupHasItem && !_hasGottenItem && _selectIndex == itemPosition)
            {
                _hasGottenItem = true;
                Instantiate(popupItem,
                    new Vector3(transform.position.x, transform.position.y, transform.position.z - 2.5f),
                    Quaternion.identity);

                if (eventToTrigger != null)
                {
                    eventToTrigger.RaiseEvent();
                }
            }
        }

        private void CloseBook()
        {
            _largeTargetSize = 0f;
            if (_hasGottenItem)
            {
                itemLargeSprites[itemPosition] = substituteSprite;
            }
        }

        public void ResetSelection()
        {
            CloseBook();
            _selectIndex = 0;
            foreach (Outline outline in _itemOutlines)
            {
                outline.enabled = false;
            }

            ResetSpriteColor();
            _itemOutlines[_selectIndex].enabled = true;
            itemSprites[_selectIndex].color = Color.white;
        }

        public void SelectNext()
        {
            CloseBook();
            _itemOutlines[_selectIndex].enabled = false;
            itemSprites[_selectIndex].color = Color.grey;
            _selectIndex++;
            if (_selectIndex == popupObjects.Count)
            {
                _selectIndex = 0;
            }

            _itemOutlines[_selectIndex].enabled = true;
            itemSprites[_selectIndex].color = Color.white;
        }

        public void SelectPrevious()
        {
            CloseBook();
            _itemOutlines[_selectIndex].enabled = false;
            itemSprites[_selectIndex].color = Color.grey;
            _selectIndex--;
            if (_selectIndex < 0)
            {
                _selectIndex = popupObjects.Count - 1;
            }

            _itemOutlines[_selectIndex].enabled = true;
            itemSprites[_selectIndex].color = Color.white;
        }

        private void ResetSpriteColor()
        {
            foreach (Image sprite in itemSprites)
            {
                sprite.color = Color.grey;
            }
        }
    }
}