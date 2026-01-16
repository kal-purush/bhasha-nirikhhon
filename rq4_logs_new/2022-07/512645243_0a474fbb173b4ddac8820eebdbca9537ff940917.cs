using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Linq;

namespace EPS {
    
    //FEATURES : BASIC SINGLETON CONTROL AND RESOURCE POOL FOR GAME OBJECTS
    public class DependencyManager
    {
        static private List<System.Type> m_registered_services = new List<System.Type>();
        static private List<EPS.Foundation.IService> m_instanced_services = new List<EPS.Foundation.IService>();
        static private List<GameObject> ResourcePool = new List<GameObject>();
        static private List<Sprite> SpriteResourcePool = new List<Sprite>();


        //TODO: probar esta funcion de abajo....(uso internamente para servicios)
        private T AutoInjectByInterface<T>(string serviceName)
        {
            return (T)this.GetService(serviceName);
        }

        public DependencyManager()
        {

        }


        //Safe start for fetching services from others
        public void ServicesOnInit()
        {
            foreach (EPS.Foundation.IService instanced in m_instanced_services)
            {
                instanced.OnInit(this);
            }
        }

        //secuential loop TODO: DESING THIS FOR MULTI TASK
        public void ServiceLoop()
        {
            foreach (EPS.Foundation.IService instanced in m_instanced_services)
            {
                instanced.Loop();
            }
        }

        public List<System.Func<IEnumerator>> GetCourrutines()
        {
            return m_instanced_services.Select((e) =>
            {
                return e.LoopCourrutine();
            }).ToList();
        }


        public static void RegisterService<T>()
        {
            System.Type type = typeof(T);
            if (!m_registered_services.Contains(type))
            {
                Debug.Log("[DM]-SERVICE REGISTERED : " + type.Name);
                m_registered_services.Add(type);
            }
        }

        //This are gonna be use for dynamic instantiated services
        public void ReloadInstances()
        {
            foreach (EPS.Foundation.IService instanced in m_instanced_services)
            {
                instanced.OnReset();
            }
        }

        public void ShutDown()
        {
            foreach (EPS.Foundation.IService instanced in m_instanced_services)
            {
                instanced.OnDestroy();
            }
            m_instanced_services.Clear();
        }


        //INIT SINGLETONS
        public void InitServices()
        {
            foreach (System.Type service in m_registered_services)
            {
                EPS.Foundation.IService instancedService = (EPS.Foundation.IService)System.Activator.CreateInstance(service);

                if (instancedService != null)
                    m_instanced_services.Add(instancedService);
                else
                    Debug.LogError("Service " + service.ToString() + " doesn't work wey");
            }
        }

        public EPS.Foundation.IService GetService(string name)
        {
            return m_instanced_services.Find((EPS.Foundation.IService e) => { return e.ReferencedName() == name; });
        }

        //RESOURCE MANAGEMENT -> all this code must be moved to another service if it gets big, and this service is a proxy
        //TO DO ARRAY OR LISTS QUERYS RESOURCES !!!!
        public void StoreResources(GameObject[] prefabs)
        {
            ResourcePool.AddRange(prefabs);
        }

        public GameObject GetResource(string name)
        {
            var findedResource = ResourcePool.Find((e) => { return e.name == name; });
            if (findedResource == null)
                throw new System.NullReferenceException($"RECURSO NO ENCONTRADO NOMBRE:  #{name}");
            else
                return findedResource;
        }
    }
}