using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;
using Unity.VisualScripting;
using Newtonsoft.Json.Linq;


public class HumanController : MonoBehaviour
{
    // Assuming this script is attached to the 'human' GameObject
    // which has the 14 joint sub-GameObjects as children.

    // Method to call when a new JSON message is received
    public void UpdateJointPositions(JArray joints)
    {

        for (int i = 0; i < joints.Count; i++)
        {
            // Construct the name of the joint GameObject
            string jointName = $"joint{i}";
            Debug.Log(joints[i][0]);

            // Find the joint GameObject as a child of 'human'
            Transform jointTransform = transform.Find(jointName);

            if (jointTransform != null)
            {
                // Update the position of the joint GameObject
                Vector3 newPosition = new Vector3(
                    (float)joints[i][0],
                    (float)joints[i][1],
                    (float)joints[i][2]
                );
                Debug.Log(newPosition);

                jointTransform.position = newPosition;
            }
            else
            {
                Debug.LogWarning($"Joint '{jointName}' not found.");
            }
        }
    }

    // Define a class to match the structure of your JSON data
    [Serializable]
    public class Joint
    {
        public float[] coords;
    }

    [Serializable]
    public class JointsData{
        public List<float> cursor_l;
        public List<float> cursor_r;
        public Joint[] joints;
        public string user;
        JointsData(List<float> cursor_l, List<float> cursor_r, Joint[] joints, string user)
        {
            this.cursor_l = cursor_l;
            this.cursor_r = cursor_r;
            this.joints = joints;
            this.user = user;
        }
    }

}