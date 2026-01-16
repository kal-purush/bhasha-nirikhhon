using UnityEngine;
using TMPro;

/// <summary>�X�}�z�̓��͂������Ă݂� </summary>
public class InputTest : MonoBehaviour
{
    [SerializeField, Tooltip("�^�b�`����Ă���ꏊ�������I�u�W�F�N�g")] GameObject _touchObjectPrefab = default;
    [SerializeField, Tooltip("�^�b�v���ꂽ�񐔂�\���@�f�o�b�O���\������ ")] TextMeshProUGUI _tapCountText = default;
    [SerializeField, Tooltip("�f�o�b�O����\��")] TextMeshProUGUI _debugText = default;
    [SerializeField, Tooltip("�{�^���������ꂽ���̃e�L�X�g")] TextMeshProUGUI _buttonDebugText = default;
    /// <summary>Instantiate�����N���[��</summary>
    GameObject _touchObject = default;
    /// <summary>�^�b�v�� </summary>
    int _tapCount = 0;
    /// <summary>�O��^�b�`���ꂽX�̈ʒu </summary>
    float _lasttouchpositionX = 0f;
    void Start()
    {
        _touchObject = Instantiate(_touchObjectPrefab, Vector2.zero, Quaternion.identity);
    }

    void Update()
    {
        foreach (var touch in Input.touches)
        {
            var touchposition = new Vector3(touch.position.x, touch.position.y, 10);
            touchposition = Camera.main.ScreenToWorldPoint(touchposition);

            if (touch.phase == TouchPhase.Moved)
            {
                _touchObject.transform.position = touchposition;

                if (touch.position.x < _lasttouchpositionX) //���Ɉړ�
                {
                    _debugText.text = "���Ɉړ�";
                }
                else if (touch.position.x > _lasttouchpositionX) //�E�Ɉړ�
                {
                    _debugText.text = "�E�Ɉړ�";
                }

                _tapCountText.text = $"{touch.position.x} {_lasttouchpositionX}";   //x�ʒu��\��
                _lasttouchpositionX = touch.position.x;
            }
        }
    }

    /// <summary>�{�^���̋@�\�������ׂ̊֐� </summary>
    public void DebugButton()
    {
        _buttonDebugText.text = "�{�^���������ꂽ";
    }
}


///���͂��󂯎��
//foreach (var touch in Input.touches)
//{
//    var touchPhase = touch.phase;

//    switch (touchPhase)
//    {
//        case TouchPhase.Began:
//            _debugText.text = "��ʂɎw���G�ꂽ";
//            break;
//        case TouchPhase.Moved:
//            _debugText.text = "��ʏ�Ŏw��������";
//            break;
//        case TouchPhase.Ended:
//            _debugText.text = "��ʂ���w�����ꂽ";
//            break;
//        case TouchPhase.Stationary:
//            _debugText.text = "�w����ʂɐG��Ă��邪�����Ă͂��Ȃ�";
//            break;
//        case TouchPhase.Canceled:
//            _debugText.text = "�V�X�e�����^�b�`�̒ǐՂ��L�����Z��";
//            break;
//        default:
//            _debugText.text = "�G��Ă��Ȃ�";
//            break;
//    }
//}

//�G��Ă���w�̐�
//if (Input.touchCount == 1)
//{
//    _debugText.text = "������Ă���";
//    _tapCount++;
//    _tapCountText.text = _tapCount.ToString();
//}

