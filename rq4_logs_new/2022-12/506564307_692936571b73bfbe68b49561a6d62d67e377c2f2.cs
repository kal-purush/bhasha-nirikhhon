using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using communication;
using tsunahiki.game;
using tsunahiki.forceGauge.state;
using tsunahiki.forceGauge.stateController;
using tsunahiki.forceGauge;


namespace tsunahiki.forceGauge
{
    public class OpponentBody : MonoBehaviour
    {
        [SerializeField]
        private Transform _opponentAvatarHead;

        [SerializeField]
        private float _lengthOfNeck;

        // Start is called before the first frame update
        void Start()
        {
            
        }

        // Update is called once per frame
        void Update()
        {
            // 胴体を頭に合わせて動かす
            this.transform.position = new Vector3(_opponentAvatarHead.position.x, _opponentAvatarHead.position.y -_lengthOfNeck, _opponentAvatarHead.position.z);
            this.transform.eulerAngles = new Vector3(0.0f, _opponentAvatarHead.eulerAngles.y, 0.0f);   
        }
    }
}