using UnityEngine;
using UnityEngine.Assertions;

namespace ZombieTiles.Mechanics.Building
{
    [RequireComponent(typeof(MeshRenderer))]
    public class BuildBase : MonoBehaviour
    {
        private Material originalMaterial;
        private MeshRenderer meshRenderer;
        private static Builder builder;

        [SerializeField]
        private Material hoverMaterial;

        private void OnMouseEnter() => meshRenderer.material = hoverMaterial;

        private void OnMouseExit() => meshRenderer.material = originalMaterial;

        private void Start()
        {
            Assert.IsNotNull(hoverMaterial);

            if (builder == null)
            {
                builder = FindObjectOfType<Builder>();
            }

            Assert.IsNotNull(builder);

            meshRenderer = GetComponent<MeshRenderer>();
            originalMaterial = meshRenderer.material;
        }

        private void OnMouseDown() => builder.RequestBuildOn(this);
    }
}