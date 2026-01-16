using KamranWali.CodeOptPro.Managers;
using System.Collections.Generic;
using UnityEngine;

namespace KamranWali.CodeOptPro.ScriptableObjects.Managers
{
    [CreateAssetMenu(fileName = "MonoAdvManager_CallHelper",
                     menuName = "CodeOptPro/ScriptableObjects/Managers/" +
                                "MonoAdvManager_CallHelper",
                     order = 1)]
    public class MonoAdvManager_CallHelper : BaseManagerHelper<MonoAdvManager_Call>
    {
        public void AddObject(MonoAdvManager obj) => manager?.AddObject(obj);
        public void SetManagers(List<MonoAdvManagerHelper> managers) { if (manager != null) manager.SetManagers(managers); }
        public List<MonoAdvManagerHelper> GetManagers() => manager != null ? manager.GetManagers() : null;
    }
}