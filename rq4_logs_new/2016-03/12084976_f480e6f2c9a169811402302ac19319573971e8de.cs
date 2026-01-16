using System;
using UnityEngine;
using Zenject;

namespace ModestTree
{
    public class TestGameObjectFactory : MonoInstallerTestFixture
    {
        public GameObject FooMonoFacadePrefab;

        [InstallerTest]
        public void Test1()
        {
            Container.BindGameObjectFactory<FooFactory>(FooMonoFacadePrefab);

            Container.BindAllInterfaces<Runner>().ToSingle<Runner>();
        }

        public class FooFactory : GameObjectFactory
        {
        }

        public class Runner : IInitializable
        {
            readonly FooFactory _factory;

            public Runner(FooFactory factory)
            {
                _factory = factory;
            }

            public void Initialize()
            {
                _factory.Create();
            }
        }
    }
}

