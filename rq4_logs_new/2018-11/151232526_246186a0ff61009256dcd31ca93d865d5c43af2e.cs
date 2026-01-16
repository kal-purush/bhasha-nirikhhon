using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using MLAgents;

namespace Gene {
    public class Cell : MonoBehaviour
    {
        [Header("Connection to API Service")]
        public bool postApiData;
        public bool requestApiData;
        [HideInInspector] public bool isRequestDone;
        [HideInInspector] public float threshold;
        [HideInInspector] public int partNb;
        [HideInInspector] public List<float> CellInfos;

        private bool initialised;
        private List<List<GameObject>> Germs;
        private List<GameObject> Cells;
        private List<Vector3> CellPositions;
        private AgentTrainBehaviour aTBehaviour;

        void Awake()
        {
            initialised = false;
        }

        void Update()
        {
            if (isRequestDone && !initialised)
            {
                initGerms(partNb, threshold);
                initialised = true;
            }
        }

        public void initGerms(int numGerms, float threshold)
        {
            PostGene postGene = new PostGene();
            // RENAME THE PARENT GAMEOBJECT//
            //transform.gameObject.name = Utils.RandomName();
            /////////////////////////////////
            // INIT GENE LIST //
            ////////////////////
            Germs = new List<List<GameObject>>();
            Cells = new List<GameObject>();
            CellPositions = new List<Vector3>();

            List<Vector3> sides = new List<Vector3>{
                new Vector3(1f, 0f, 0f),
                new Vector3(0f, 1f, 0f),
                new Vector3(0f, 0f, 1f),
                new Vector3(-1f, 0f, 0f),
                new Vector3(0f, -1f, 0f),
                new Vector3(0f, 0f, -1f)
            };

            //////////////////////////////////////////////////////////////////////////////////////
            ////////////////////////////////INIT BASE GERMS///////////////////////////////////////
            //////////////////////////////////////////////////////////////////////////////////////
            /// 1ST CELL ///
            // init object shape
            Germs.Add(new List<GameObject>());
            GameObject initCell = InitBaseShape(Germs[0], 0);
            InitRigidBody(initCell);
            HandleStoreCell(initCell);
            //////////////////////////////////////////////////////////////////////////////////////


            //////////////////////////////////////////////////////////////////////////////////////
            /////////////////// Iterate for each new part of the morphology //////////////////////
            /// //////////////////////////////////////////////////////////////////////////////////
            int x = 0;
            for (int y = 1; y < numGerms; y++)
            {
                int prevCount = Germs[y - 1].Count;
                Germs.Add(new List<GameObject>());
                //////////////////////////////////////////////////
                /// ITERATE FOR EACH PREVIOUS GERM CELL NUMBER ///
                for (int i = 0; i < prevCount; i++)
                {
                    //////////////////////////////////////////////////
                    ////////// ITERATE FOR EACH CELL SIDES ///////////
                    for (int z = 0; z < sides.Count; z++)
                    {
                        bool isValid = true;
                        float CellInfo = Random.Range(0f, 1f);
                        Vector3 cellPosition = Germs[y - 1][i].transform.position + sides[z];

                        CellInfo = HandleCellsRequest(x, CellInfo);

                        isValid = CheckIsValid(isValid, cellPosition);
                        if (isValid)
                        {
                            if (CellInfos[x] > threshold)
                            {
                                GameObject cell = InitBaseShape(Germs[y], y);
                                InitPosition(sides, y, i, z, cell);
                                InitRigidBody(cell);
                                initJoint(cell, Germs[y - 1][i], sides[z]);
                                HandleStoreCell(cell);
                            }
                        }
                        x++;
                    }
                }

                foreach (var cell in Cells)
                {
                    cell.transform.parent = transform;
                }
            }

            foreach (var cell in Cells)
            {
                cell.transform.localScale *= 2f;
                cell.GetComponent<SphereCollider>().radius /= 2f;
            }

            //////////////////////////////////////////////////////////////////////////////////////
            string postData = HandlePostData();
            AddAgentPart();

            if (postApiData)
            {
                ////Post data to Api
                //GameObject.Find("Focus Camera").GetComponent<WebCamPhotoCamera>().CaptureScreenshot();
                StartCoroutine(postGene.postCell(postData, transform.gameObject.name));
            }

        }

        private void HandleStoreCell(GameObject cell)
        {
            Cells.Add(cell);
            CellPositions.Add(cell.transform.localPosition);
        }

        private string HandlePostData()
        {
            string postData = "";
            foreach (var info in CellInfos)
            {
                postData = postData + 'A' + info.ToString();
            }

            return postData;
        }

        private float HandleCellsRequest(int x, float CellInfo)
        {
            if (requestApiData)
            {
                CellInfo = CellInfos[x];
                CellInfos.Add(CellInfo);
            }
            else
            {
                CellInfos.Add(CellInfo);
            }

            return CellInfo;
        }

        private static void InitRigidBody(GameObject cell)
        {
            cell.AddComponent<Rigidbody>();
            cell.GetComponent<Rigidbody>().mass = 1f;
        }

        private void InitPosition(List<Vector3> sides, int y, int i, int z, GameObject cell)
        {
            cell.transform.parent = Germs[y - 1][i].transform;
            cell.transform.localPosition = sides[z];
        }

        private GameObject InitBaseShape(List<GameObject> germs, int y)
        {
            germs.Add(GameObject.CreatePrimitive(PrimitiveType.Sphere));
            GameObject cell = Germs[y][Germs[y].Count - 1];
            cell.GetComponent<Renderer>().material = Resources.Load<Material>("Materials/cell/cell");
            cell.GetComponent<Renderer>().material.SetVector("_REMAPX", new Vector2(Random.Range(0f, 0.5f), Random.Range(0.5f, 1f)));
            cell.GetComponent<Renderer>().material.SetVector("_REMAPY", new Vector2(Random.Range(0f, 0.5f), Random.Range(0.5f, 1f)));
            return cell;
        }

        private bool CheckIsValid(bool isValid, Vector3 cellPosition)
        {
            foreach (var position in CellPositions)
            {
                if (cellPosition == position)
                {
                    isValid = !isValid;
                }
            }

            return isValid;
        }

        private void initJoint(GameObject part, GameObject connectedBody, Vector3 jointAnchor)
        {
            ConfigurableJoint cj = part.transform.gameObject.AddComponent<ConfigurableJoint>();
            ///////////////////////
            // Configurable Joint Motion 
            cj.xMotion = ConfigurableJointMotion.Locked;
            cj.yMotion = ConfigurableJointMotion.Locked;
            cj.zMotion = ConfigurableJointMotion.Locked;
            // Configurable Joint Angular Mortion
            cj.angularXMotion = ConfigurableJointMotion.Limited;
            cj.angularYMotion = ConfigurableJointMotion.Limited;
            cj.angularZMotion = ConfigurableJointMotion.Limited;
            // Configurable Joint Connected Body AND Anchor settings
            cj.connectedBody = connectedBody.gameObject.GetComponent<Rigidbody>();
            cj.rotationDriveMode = RotationDriveMode.Slerp;
            // cj.anchor = jointAnchor;
            // cj.axis = partCoOrd.jointAxis;
            // Configurable Joint Angular Limit
            // Important to have 0 of bounciness
            cj.angularYLimit = new SoftJointLimit() { limit = Random.Range(0f, 40f), bounciness = 0f };
            cj.highAngularXLimit = new SoftJointLimit() { limit = Random.Range(0f, 90f), bounciness = 0f };
            cj.lowAngularXLimit = new SoftJointLimit() { limit = 0f, bounciness = 0f };
            part.gameObject.GetComponent<Rigidbody>().useGravity = true;
        }

        private void AddAgentPart()
        {
            aTBehaviour = transform.gameObject.GetComponent<AgentTrainBehaviour>();
            aTBehaviour.initPart = Cells[0].transform;
            for (int i = 1; i < Cells.Count; i++)
            {
                aTBehaviour.parts.Add(Cells[i].transform);
            }
            aTBehaviour.initBodyParts();
        }
    }
}