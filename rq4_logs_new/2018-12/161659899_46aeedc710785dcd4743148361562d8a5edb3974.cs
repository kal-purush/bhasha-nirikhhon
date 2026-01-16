using NUnit.Framework;
using UnityEditor;
using UnityEngine;

namespace InitialPrefabs.Tests.Attributes {

    public abstract class BaseAttributeGUITests {

        protected GameObject testObject;
        protected SerializedObject serializedObject;

        [SetUp]
        public abstract void SetUp();

        [TearDown]
        public virtual void TearDown() => Object.DestroyImmediate(testObject);

        protected void AssignTestObject<T>() where T : Component {
            testObject = new GameObject("[Test Object");
            serializedObject = new SerializedObject(testObject.AddComponent<T>());
        }
    }
}