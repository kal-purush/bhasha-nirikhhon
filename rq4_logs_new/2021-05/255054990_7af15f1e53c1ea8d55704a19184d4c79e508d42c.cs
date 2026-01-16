using Sim.Enums;
using UnityEngine;

namespace Sim.Scriptables {
    [CreateAssetMenu(fileName = "New Cover", menuName = "Configurations/Cover")]
    public class CoverConfig : ScriptableObject {
        [SerializeField]
        private int id;

        [SerializeField]
        private string displayName;

        [SerializeField]
        private Sprite sprite;

        [SerializeField]
        private Material material;

        [SerializeField]
        private PropsConfig bucketPropsConfig;

        [SerializeField]
        private BuildSurfaceEnum surface;

        [SerializeField]
        private bool customColor;

        [SerializeField]
        private bool buyable;

        [SerializeField]
        private int price;

        public int GetId() {
            return this.id;
        }

        public int Price => price;
        
        public bool IsBuyable() => buyable;

        public string GetDisplayName() {
            return this.displayName;
        }

        public Material GetMaterial() {
            return this.material;
        }

        public BuildSurfaceEnum GetSurface() {
            return this.surface;
        }

        public bool AllowCustomColor() {
            return this.customColor;
        }

        public PropsConfig GetBucketPropsConfig() {
            return this.bucketPropsConfig;
        }

        public bool IsWallCover() {
            return this.surface == BuildSurfaceEnum.WALL;
        }

        public bool IsGroundCover() {
            return this.surface == BuildSurfaceEnum.GROUND;
        }

        public Sprite Sprite => sprite;
    }
}