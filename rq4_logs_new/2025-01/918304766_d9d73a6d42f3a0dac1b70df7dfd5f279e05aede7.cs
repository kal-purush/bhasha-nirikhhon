using UnityEngine;

public class IKController : MonoBehaviour
{
    [SerializeField] private Animator _animator; 
    [SerializeField] private Transform _boxTransform; 
    [SerializeField] [Range(1,5)]  private float _boxWeight = 1f; 
    [SerializeField] private Rigidbody _boxRigidbody;
    [SerializeField] private Transform _handRight;
    [SerializeField] private Transform _handLeft;
    
    private bool _boxIsCaptured = false;
    
    private const float MaxBoxWeight = 10f; 

    private void Awake()
    {
        _boxRigidbody.mass = _boxWeight;
    }

    void OnAnimatorIK(int layerIndex)
    {
        if (_boxIsCaptured)
        {
            float weight = Mathf.Clamp01(_boxWeight / 10f); 
            
            _animator.SetIKPositionWeight(AvatarIKGoal.RightHand, weight);
            _animator.SetIKRotationWeight(AvatarIKGoal.RightHand, weight);
            _animator.SetIKPosition(AvatarIKGoal.RightHand, _handRight.position);
            _animator.SetIKRotation(AvatarIKGoal.RightHand, _handRight.rotation);
            
            _animator.SetIKPositionWeight(AvatarIKGoal.LeftHand, weight);
            _animator.SetIKRotationWeight(AvatarIKGoal.LeftHand, weight);
            _animator.SetIKPosition(AvatarIKGoal.LeftHand, _handLeft.position);
            _animator.SetIKRotation(AvatarIKGoal.LeftHand, _handLeft.rotation);
        }
        else
        {
            _animator.SetIKPositionWeight(AvatarIKGoal.RightHand, 0f);
            _animator.SetIKRotationWeight(AvatarIKGoal.RightHand, 0f);
            _animator.SetIKPositionWeight(AvatarIKGoal.LeftHand, 0f);
            _animator.SetIKRotationWeight(AvatarIKGoal.LeftHand, 0f);
        }
    }
    
    public void StartInteraction()
    {
        _boxIsCaptured = true;
        _boxRigidbody.isKinematic = true;
        _boxTransform.transform.SetParent(_handRight);
    }

    public void ThrowBox()
    {
        _boxIsCaptured = false;
        _boxTransform.transform.SetParent(null);
        _boxRigidbody.isKinematic = false;
        _boxRigidbody.AddForce(_handRight.right * MaxBoxWeight/_boxWeight, ForceMode.VelocityChange); 
    }
}