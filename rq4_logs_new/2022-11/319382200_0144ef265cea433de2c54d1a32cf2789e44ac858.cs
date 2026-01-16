using System;
using System.Collections.Generic;
using UGF.EditorTools.Runtime.Assets;
using UGF.EditorTools.Runtime.Ids;
using UnityEngine;

namespace UGF.Models.Runtime.Collections
{
    [CreateAssetMenu(menuName = "Unity Game Framework/Models/Collection Dictionary Model", order = 2000)]
    public class CollectionDictionaryModelAsset : ModelAsset
    {
        [SerializeField] private List<Entry> m_items = new List<Entry>();

        public List<Entry> Items { get { return m_items; } }

        [Serializable]
        public struct Entry
        {
            [AssetId]
            [SerializeField] private GlobalId m_key;
            [AssetId]
            [SerializeField] private GlobalId m_value;

            public GlobalId Key { get { return m_key; } set { m_key = value; } }
            public GlobalId Value { get { return m_value; } set { m_value = value; } }
        }

        protected override IModel OnBuild()
        {
            var model = new CollectionDictionaryModel();

            for (int i = 0; i < m_items.Count; i++)
            {
                Entry entry = m_items[i];

                if (!entry.Key.IsValid()) throw new ArgumentException("Value should be valid.", nameof(entry.Key));
                if (!entry.Value.IsValid()) throw new ArgumentException("Value should be valid.", nameof(entry.Value));

                model.Items.Add(entry.Key, entry.Value);
            }

            return model;
        }
    }
}