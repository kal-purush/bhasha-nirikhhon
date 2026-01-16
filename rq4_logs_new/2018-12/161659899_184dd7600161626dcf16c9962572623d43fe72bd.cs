using InitialPrefabs.Attributes.Messages;
using InitialPrefabs.Examples;
using NUnit.Framework;
using UnityEditor;
using UnityEngine;

using static NUnit.Framework.Assert;
using static UnityEditor.EditorGUI;
using static UnityEditor.EditorGUIUtility;
using static UnityEngine.Object;

namespace InitialPrefabs.Tests.Attributes {

    public class MsgAttributeGUITests {

        private GameObject testObject;
        private MonoBehaviourExample example;
        private MsgAttribute stdMsgAttr;
        private SerializedObject serializedObject;

        [SetUp]
        public void SetUp() {
            testObject       = new GameObject("[Test Object]");
            example          = testObject.AddComponent<MonoBehaviourExample>();
            IsNotNull(example, "No MonoBehaviourExample was attached the gameObject!");
            serializedObject = new SerializedObject(example);
            stdMsgAttr       = new MsgAttribute("Test");
        }

        [TearDown]
        public void TearDown() => DestroyImmediate(testObject);

        /// <summary>
        /// Ensures that a standard msg attribute has exactly the height given on the inspector.
        /// </summary>
        [Test]
        public void StandardMsgAttributeGUIHeightTest() {
            var intMsgProp = serializedObject.FindProperty("defaultMsg");

            IsNotNull(intMsgProp, $"{testObject.name} does not have an intMsg field!");
            var currentHeight = GetPropertyHeight(intMsgProp);
            AreEqual(stdMsgAttr.height * singleLineHeight, GetPropertyHeight(intMsgProp), "Height mismatch!");
        }

        /// <summary>
        /// Get the customMsgHeight field to ensure that it manipulates the height in the inspector.
        /// </summary>
        [Test]
        public void CustomMsgAttributeGUIHeightTest() {
            var charMsgProp = serializedObject.FindProperty("customMsgHeight");
            IsNotNull(charMsgProp, $"{testObject.name} does not have an charMsg field!");

            var currentHeight = GetPropertyHeight(charMsgProp);
            Greater(currentHeight, stdMsgAttr.height * singleLineHeight, "Height is smaller than the standard height!");
        }
    }
}