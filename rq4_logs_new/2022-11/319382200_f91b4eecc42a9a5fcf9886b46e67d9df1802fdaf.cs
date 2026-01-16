using System;
using System.Collections.Generic;
using UGF.EditorTools.Runtime.Assets;
using UGF.EditorTools.Runtime.Ids;
using UnityEngine;

namespace UGF.Models.Runtime.Domain
{
    [CreateAssetMenu(menuName = "Unity Game Framework/Models/Domain Model Collection Model", order = 2000)]
    public class DomainModelCollectionModelAsset : ModelAsset
    {
        [AssetId(typeof(ModelAsset))]
        [SerializeField] private List<GlobalId> m_models = new List<GlobalId>();

        public List<GlobalId> Models { get { return m_models; } }

        protected override IModel OnBuild()
        {
            var model = new DomainModelCollectionModel();

            for (int i = 0; i < m_models.Count; i++)
            {
                GlobalId id = m_models[i];

                if (!id.IsValid()) throw new ArgumentException("Value should be valid.", nameof(id));

                model.ModelIds.Add(id);
            }

            return model;
        }
    }
}