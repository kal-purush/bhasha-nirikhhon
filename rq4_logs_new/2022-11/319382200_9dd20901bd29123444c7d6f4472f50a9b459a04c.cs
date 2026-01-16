using System;
using System.Collections.Generic;
using UGF.EditorTools.Runtime.Assets;
using UGF.EditorTools.Runtime.Ids;
using UnityEngine;

namespace UGF.Models.Runtime.Domain
{
    [CreateAssetMenu(menuName = "Unity Game Framework/Models/Domain Model Group Model", order = 2000)]
    public class DomainModelGroupModelAsset : ModelAsset
    {
        [SerializeField] private List<Entry> m_models = new List<Entry>();

        public List<Entry> Models { get { return m_models; } }

        [Serializable]
        public struct Entry
        {
            [AssetId(typeof(ModelAsset))]
            [SerializeField] private GlobalId m_key;
            [AssetId(typeof(ModelAsset))]
            [SerializeField] private GlobalId m_value;

            public GlobalId Key { get { return m_key; } set { m_key = value; } }
            public GlobalId Value { get { return m_value; } set { m_value = value; } }
        }

        protected override IModel OnBuild()
        {
            var model = new DomainModelGroupModel();

            for (int i = 0; i < m_models.Count; i++)
            {
                Entry entry = m_models[i];

                if (!entry.Key.IsValid()) throw new ArgumentException("Value should be valid.", nameof(entry.Key));
                if (!entry.Value.IsValid()) throw new ArgumentException("Value should be valid.", nameof(entry.Value));

                model.ModelIds.Add(entry.Key, entry.Value);
            }

            return model;
        }
    }
}