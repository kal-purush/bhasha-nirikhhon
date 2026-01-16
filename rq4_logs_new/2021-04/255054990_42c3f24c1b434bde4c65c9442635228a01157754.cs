using System.Collections;
using System.Linq;
using Mirror;
using Sim.Building;
using Sim.Entities;
using Sim.Utils;
using UnityEngine;
using UnityEngine.AI;
using UnityEngine.Networking;

namespace Sim {
    public class ApartmentController : NetworkBehaviour {
        [Header("Settings")]
        [SerializeField]
        private FrontDoor frontDoor;

        [SerializeField]
        private Transform originPoint;

        [Header("Only for debug")]
        [SerializeField]
        private Home homeData;

        [SerializeField]
        private Address address;

        [SerializeField]
        private string tenantId;

        [SerializeField]
        private bool isGenerated;

        public delegate void GenerateResponse(bool isGenerated);

        public event GenerateResponse OnApartmentGenerated;

        [Server]
        public void Save(SceneData sceneData) {
            ApiManager.Instance.SaveHomeScene(this.homeData, sceneData);
        }

        [Server]
        public void Init(Address newAddress) {
            this.address = newAddress;
            this.frontDoor.Number = newAddress.DoorNumber;

            StartCoroutine(RetrieveData());
        }

        [Server]
        private IEnumerator RetrieveData() {
            UnityWebRequest request = ApiManager.Instance.RetrieveHomeRequest(this.address);

            yield return request.SendWebRequest();

            Home homeResponse = JsonUtility.FromJson<Home>(request.downloadHandler.text);

            this.isGenerated = true;
            
            OnApartmentGenerated?.Invoke(true);
            
            if (homeResponse != null) {
                Debug.Log($"Home found for Address {address}");
                this.homeData = homeResponse;
                //InstantiateLevel(homeResponse.SceneData);
            } else {
                Debug.Log($"No Home found for Address {address}");
            }
        }

        [Server]
        private void InstantiateLevel(SceneData sceneData) {
            // Instantiate all grounds
            sceneData.grounds?.ToList().ForEach(data => {
                Ground props = SaveUtils.InstantiatePropsFromSave(data, originPoint.position) as Ground;
                props.Init(data.paintConfigId);
                props.transform.parent = this.transform;
            });

            // Instantiate all walls
            /*sceneData.walls?.ToList().ForEach(data => {
                Wall props = SaveUtils.InstantiatePropsFromSave(data) as Wall;
                props.Init(JsonHelper.ToJson(data.wallFaces));
            });*/

            // Instantiate all simple doors
            sceneData.simpleDoors?.ToList().ForEach(data => {
                Props props = SaveUtils.InstantiatePropsFromSave(data, originPoint.position);
                props.transform.parent = this.transform;
            });

            // Instantiate all buckets
            sceneData.buckets?.ToList().ForEach(data => {
                PaintBucket props = SaveUtils.InstantiatePropsFromSave(data, originPoint.position) as PaintBucket;
                props.Init(data.paintConfigId, data.color);
                props.transform.parent = this.transform;
            });

            // Instantiate all other props
            sceneData.props?.ToList().ForEach(data => {
                Props props = SaveUtils.InstantiatePropsFromSave(data, originPoint.position);
                props.transform.parent = this.transform;
            });

            this.isGenerated = true;
            
            OnApartmentGenerated?.Invoke(true);
        }

        public bool IsGenerated => isGenerated;

        public Home HomeData {
            get => homeData;
            set => homeData = value;
        }

        public bool IsTenant(CharacterData character) {
            return character.Id == this.tenantId;
        }
    }
}