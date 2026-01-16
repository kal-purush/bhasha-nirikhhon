using System.Collections.Generic;
using TMPro;
using UMSAGL.Scripts;
using UnityEngine;
using Visualization;
using Visualization.ClassDiagram;
using Visualization.ClassDiagram.Diagrams;
using Visualization.ClassDiagram.Relations;

namespace AnimArch.Visualization.Diagrams
{
    public class ActivityDiagram : Diagram
    {
        public Graph graph;

        private void Awake()
        {
            DiagramPool.Instance.ActivityDiagram = this;
            ResetDiagram();
        }

        public void ResetDiagram()
        {
            if (DiagramPool.Instance.ClassDiagram && DiagramPool.Instance.ClassDiagram.Classes != null)
            {
                foreach (var classDiagramClass in DiagramPool.Instance.ClassDiagram.Classes)
                {
                    classDiagramClass.ClassInfo.Instances.Clear();
                }
            }

            if (DiagramPool.Instance.RelationsClassToObject != null)
            {
                foreach (InterGraphRelation igr in DiagramPool.Instance.RelationsClassToObject)
                {
                    Destroy(igr);
                    igr.Destroy();
                }

                DiagramPool.Instance.RelationsClassToObject = new List<InterGraphRelation>();
            }

            if (graph != null)
            {
                Destroy(graph.gameObject);
                graph = null;
            }
        }

        public void LoadDiagram()
        {
            CreateGraph();
            //Generate UI objects displaying the diagram
            Generate();

            //Set the layout of diagram so it is corresponding to EA view
            ManualLayout();
            //AutoLayout();

            graph.transform.position = new Vector3(0, 0, 2*offsetZ);
        }

        private Graph CreateGraph()
        {
            ResetDiagram();
            var go = Instantiate(DiagramPool.Instance.graphPrefab);
            graph = go.GetComponent<Graph>();
            graph.nodePrefab = DiagramPool.Instance.activityPrefab;
            return graph;
        }

        public void ManualLayout()  // TODO: implement manual layout
        {
        }
        private void Generate()
        {
            //Render classes
            GenerateActivity("Activity1");
        }

        private void GenerateActivity(string name)
        {
            //Setting up
            var node = graph.AddNode();
            node.GetComponent<Clickable>().IsObject = true;
            node.SetActive(true);
            node.name = name;
            var background = node.transform.Find("Background");
            var header = background.Find("Header");

            // Printing the values into diagram
            header.GetComponent<TextMeshProUGUI>().text = node.name;
        }
    }
}