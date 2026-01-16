using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class KrakenAttack : MonoBehaviour {
        public CircleCollider2D cc;

        public float startRadius;
        public float endRadius;
        public float expandSpeed;
        public float magnitude;

        // Use this for initialization
        void Start() {

        }

        // Update is called once per frame
        void Update() {
            StartCoroutine(GrabAndSink(startRadius, endRadius));
        }


    // Copy of the ExpandAndKill IEnumerator set up in WaveExpand, changing to a smaller radius and trying to add a transparent circle to display to user. Also need to root the ship once the Kraken takes hold
        IEnumerator GrabAndSink(float startRad, float endRad) {
            for (float i = startRad; i < endRad; i = i + expandSpeed * Time.deltaTime) {
                //float time = (endRadius - startRadius) / expandSpeed * Time.deltaTime;
                //magnitude = Mathf.Lerp();
                //ps.shape.radius = i;
                cc.radius = i;
                yield return null;
            }
        }
    }