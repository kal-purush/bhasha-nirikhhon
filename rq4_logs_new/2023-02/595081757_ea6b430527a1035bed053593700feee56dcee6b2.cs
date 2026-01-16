using System.Collections;
using UnityEngine;

namespace Audio.Ambient
{
    public class AmbientSoundMaster : MonoBehaviour
    {
        [SerializeField] private SfxSO ambientSoundObject;

        private void Start()
        {
            StartCoroutine(PlaySoundDelay());
        }

        private IEnumerator PlaySoundDelay()
        {
            yield return new WaitForSeconds(Random.Range(25, 60));
            
            SoundManager.Instance.PlaySfx(ambientSoundObject);
            StartCoroutine(PlaySoundDelay());
        }
    }
}