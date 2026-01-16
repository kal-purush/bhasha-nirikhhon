using UnityEngine;
using UnityEditor;
using System;
using System.Xml;
using System.Collections;
using System.Collections.Generic;
/// <summary>
/// ------------- Camera Config Window ------------------------
/// Generates and edits helper points for camera states on all objs
/// with IsPlayer script.
/// Author: Julie Maksymova
/// Last Edited Date: August 26 / 2015
/// </summary>
public class CameraConfigWindow : EditorWindow
{
   // window scroll
   private Vector2 scrollPos = Vector2.zero;
   private bool showSettings = true;
   private string status = "Select a Player";

   // Global camera settings --------------------------
   private float maxSliderRange = 4f;
   private float chaseRotationSpeed = 1f;
   private float zoomRotationSpeed = 1f;
   private float runRotationSpeed = 1f;
   private bool invertY = false;

   // XML saving --------------------------------------
   private string filePath_global = "CameraGlobalSettings.xml";
   private string filePath_helpers = "CameraHelperObjSettings.xml";

   // Camera helper points local temp -----------------
   GameObject focusPoint = null;
   GameObject zoomPoint = null;
   GameObject chasePoint = null;
   GameObject raycastPoint = null;
   GameObject runPoint = null;

   [MenuItem("Game Configurations/Camera Config")]
   static void Init()
   {
      EditorWindow window = (CameraConfigWindow)EditorWindow.GetWindow(typeof(CameraConfigWindow));
      window.maxSize = new Vector2(500, 600);
      window.minSize = window.maxSize;
      window.Show();
   }

   void OnEnable()
   {
      // load global rotation settings when window is opened
      ReadGlobalSettingsXML();
      ReadHelpersXML();
   }

   void OnInspectorUpdate()
   {
      this.Repaint();
   }

   void OnGUI()
   {
      GUILayout.Label("Camera Configuration", EditorStyles.boldLabel);
      GenerateHelpers();
      EditGlobalSettings();
      AdjustHelpers();
      SaveSettings();
   }

   /// <summary>
   /// Edit global camera settings - rotation speed in each state, inverted axis.
   /// </summary>
   private void EditGlobalSettings()
   {
      GUILayout.BeginHorizontal();
      GUILayout.Label("Global Camera Settings", EditorStyles.boldLabel);
      GUILayout.EndHorizontal();

      // max adjustment range for helper point sliders
      GUILayout.BeginVertical();
      maxSliderRange = EditorGUILayout.FloatField("Max slider range:", maxSliderRange);

      // boundary check
      if (maxSliderRange < 2f || maxSliderRange > 6f)
      {
         maxSliderRange = 2f;
      }

      // rotation speed sliders
      chaseRotationSpeed = EditorGUILayout.Slider("Chase rotation speed:", chaseRotationSpeed, 1f, 20f);
      zoomRotationSpeed = EditorGUILayout.Slider("Zoom rotation speed:", zoomRotationSpeed, 1f, 20f);
      runRotationSpeed = EditorGUILayout.Slider("Run rotation speed:", runRotationSpeed, 1f, 20f);
      // set Y axis invert
      CameraCtrl.Instance.invertY = EditorGUILayout.Toggle("Invert Y axis", CameraCtrl.Instance.invertY);
      GUILayout.EndVertical();

      EditorGUILayout.Space();
   }

   /// <summary>
   /// Generates helper points at default positions.
   /// </summary>
   private void GenerateHelpers()
   {
      if (GUILayout.Button("Generate Helper Points"))
      {
         // get all player characters
         IsPlayer[] playerChars = GameObject.FindObjectsOfType<IsPlayer>();
         // if missing add camera helpers at default positions to characters
         foreach (IsPlayer character in playerChars)
         {
            if (focusPoint == null)
            {
               // attach FocusPoint
               focusPoint = new GameObject();
               focusPoint.transform.parent = character.transform;
               focusPoint.gameObject.name = "FocusPoint";
               focusPoint.AddComponent<IsCamHelperPoint>();

               Vector3 focusPosition = new Vector3(0.3958f, 1.113f, 0f);
               focusPoint.transform.position = character.transform.TransformPoint(focusPosition);
            }
            else
            {
               focusPoint = UtilityScript.FindChildByName(character.transform, "FocusPoint").gameObject;
            }

            if (zoomPoint == null)
            {
               // attach ZoomPoint
               zoomPoint = new GameObject();
               zoomPoint.transform.parent = character.transform;
               zoomPoint.gameObject.name = "ZoomPoint";
               zoomPoint.AddComponent<IsCamHelperPoint>();

               Vector3 zoomPosition = new Vector3(0.396f, 1.113f, -0.549f);
               zoomPoint.transform.position = character.transform.TransformPoint(zoomPosition);
            }
            else
            {
               zoomPoint = UtilityScript.FindChildByName(character.transform, "ZoomPoint").gameObject;
            }

            if (chasePoint == null)
            {
               // attach ChasePoint
               chasePoint = new GameObject();
               chasePoint.transform.parent = character.transform;
               chasePoint.gameObject.name = "ChasePoint";
               chasePoint.AddComponent<IsCamHelperPoint>();

               Vector3 chasePosition = new Vector3(0.458f, 1.215f, -1.373f);
               chasePoint.transform.position = character.transform.TransformPoint(chasePosition);
            }
            else
            {
               chasePoint = UtilityScript.FindChildByName(character.transform, "ChasePoint").gameObject;
            }

            if (runPoint == null)
            {
               // attach RunPoint
               runPoint = new GameObject();
               runPoint.transform.parent = character.transform;
               runPoint.gameObject.name = "RunPoint";
               runPoint.AddComponent<IsCamHelperPoint>();

               Vector3 runPosition = new Vector3(0.458f, 1.215f, -2.034f);
               runPoint.transform.position = character.transform.TransformPoint(runPosition);
            }
            else
            {
               runPoint = UtilityScript.FindChildByName(character.transform, "RunPoint").gameObject;
            }

            if (raycastPoint == null)
            {
               // attach raycastPoint
               raycastPoint = UtilityScript.FindChildByName(character.transform, "Spine").gameObject;
               if (raycastPoint != null)
               {
                  if (raycastPoint.GetComponent<IsRaycastPoint>() == null)
                  {
                     raycastPoint.AddComponent<IsRaycastPoint>();
                  }
               }
               else
               {
                  Debug.LogError("Transform 'Spine' was not found in parent " + character.name);
               }
            }
         }
         EditorUtility.DisplayDialog("CameraConfig", "Camera helper points generated.", "OK");
      }
   }

   /// <summary>
   /// Edit helper points positions.
   /// </summary>
   private void AdjustHelpers()
   {
      showSettings = EditorGUILayout.Foldout(showSettings, status, EditorStyles.boldLabel);
      if (showSettings)
      {
         if (Selection.activeTransform)
         {
            // if selected object is a Player's character
            if (Selection.activeTransform.gameObject.GetComponent<IsPlayer>())
            {
               status = Selection.activeTransform.name;

               // set helpers once
               if (focusPoint == null)
               {
                  ReadHelpersXML();
               }

               // display sliders for editing
               EditorGUI.indentLevel++;
               GUILayout.Label("FocusPoint", EditorStyles.label);
               if (focusPoint != null)
               {
                  float focusX = focusPoint.transform.localPosition.x;
                  float focusY = focusPoint.transform.localPosition.y;
                  float focusZ = focusPoint.transform.localPosition.z;

                  focusX = EditorGUILayout.Slider("FocusPoint X", focusX, -maxSliderRange, maxSliderRange);
                  focusY = EditorGUILayout.Slider("FocusPoint Y", focusY, -maxSliderRange, maxSliderRange);
                  focusZ = EditorGUILayout.Slider("FocusPoint Z", focusZ, -maxSliderRange, maxSliderRange);
                  focusPoint.transform.localPosition = new Vector3(focusX, focusY, focusZ);
               }
               GUILayout.Label("ZoomPoint", EditorStyles.label);
               if (zoomPoint != null)
               {
                  float zoomX = zoomPoint.transform.localPosition.x;
                  float zoomY = zoomPoint.transform.localPosition.y;
                  float zoomZ = zoomPoint.transform.localPosition.z;

                  zoomX = EditorGUILayout.Slider("ZoomPoint X", zoomX, -maxSliderRange, maxSliderRange);
                  zoomY = EditorGUILayout.Slider("ZoomPoint Y", zoomY, -maxSliderRange, maxSliderRange);
                  zoomZ = EditorGUILayout.Slider("ZoomPoint Z", zoomZ, -maxSliderRange, maxSliderRange);
                  zoomPoint.transform.localPosition = new Vector3(zoomX, zoomY, zoomZ);
               }
               GUILayout.Label("ChasePoint", EditorStyles.label);
               if (chasePoint != null)
               {
                  float chaseX = chasePoint.transform.localPosition.x;
                  float chaseY = chasePoint.transform.localPosition.y;
                  float chaseZ = chasePoint.transform.localPosition.z;

                  chaseX = EditorGUILayout.Slider("ChasePoint X", chaseX, -maxSliderRange, maxSliderRange);
                  chaseY = EditorGUILayout.Slider("ChasePoint Y", chaseY, -maxSliderRange, maxSliderRange);
                  chaseZ = EditorGUILayout.Slider("ChasePoint Z", chaseZ, -maxSliderRange, maxSliderRange);
                  chasePoint.transform.localPosition = new Vector3(chaseX, chaseY, chaseZ);
               }
               GUILayout.Label("RunPoint", EditorStyles.label);
               if (runPoint != null)
               {
                  float runX = runPoint.transform.localPosition.x;
                  float runY = runPoint.transform.localPosition.y;
                  float runZ = runPoint.transform.localPosition.z;

                  runX = EditorGUILayout.Slider("RunPoint X", runX, -maxSliderRange, maxSliderRange);
                  runY = EditorGUILayout.Slider("RunPoint Y", runY, -maxSliderRange, maxSliderRange);
                  runZ = EditorGUILayout.Slider("RunPoint Z", runZ, -maxSliderRange, maxSliderRange);
                  runPoint.transform.localPosition = new Vector3(runX, runY, runZ);
               }
               EditorGUI.indentLevel--;
               EditorGUILayout.Space();

               // button to reset defaults 
               if (GUILayout.Button("Reset helper positions to default values"))
               {
                  if (focusPoint != null)
                  {
                     focusPoint.transform.localPosition = new Vector3(0.39f, 1.11f, 0f);
                  }

                  if (zoomPoint != null)
                  {
                     zoomPoint.transform.localPosition = new Vector3(0.39f, 1.11f, -0.54f);
                  }

                  if (chasePoint != null)
                  {
                     chasePoint.transform.localPosition = new Vector3(0.39f, 1.11f, -1.37f);
                  }

                  if (runPoint != null)
                  {
                     runPoint.transform.localPosition = new Vector3(0.39f, 1.11f, -2.03f);
                  }
               }
            }
         }
      }

      if (!Selection.activeTransform)
      {
         status = "Select a Character with IsPlayer script.";
         showSettings = false;
      }
   }

   private void SaveSettings()
   {
      if (GUILayout.Button("Save Settings"))
      {
         SaveGlobalSettingsXML();
         SaveHelpersXML();

         // read settings from XML
         ReadGlobalSettingsXML();
         ReadHelpersXML();
         EditorUtility.DisplayDialog("CameraConfig", "Camera settings saved.", "OK");
      }
   }

   /// <summary>
   /// Saves settings to XML for editing in Game mode.
   /// </summary>
   private void SaveGlobalSettingsXML()
   {
      // set XML formatting
      XmlWriterSettings xml_format = new XmlWriterSettings();
      xml_format.Indent = true;
      xml_format.IndentChars = ("\t");

      using (XmlWriter writer = XmlWriter.Create(filePath_global, xml_format))
      {
         writer.WriteStartDocument();
         writer.WriteStartElement("GlobalSettings");

         writer.WriteStartElement("chaseRotationSpeed");
         writer.WriteString(chaseRotationSpeed.ToString());
         writer.WriteEndElement();

         writer.WriteStartElement("zoomRotationSpeed");
         writer.WriteString(zoomRotationSpeed.ToString());
         writer.WriteEndElement();

         writer.WriteStartElement("runRotationSpeed");
         writer.WriteString(runRotationSpeed.ToString());
         writer.WriteEndElement();

         writer.WriteStartElement("invertY");
         writer.WriteString(CameraCtrl.Instance.invertY.ToString());
         writer.WriteEndElement();

         writer.WriteEndElement();
         writer.WriteEndDocument();
      }
   }

   /// <summary>
   /// Saves helper obj positions to XML for editing in Game mode.
   /// </summary>
   private void SaveHelpersXML()
   {
      // set XML formatting
      XmlWriterSettings xml_format = new XmlWriterSettings();
      xml_format.Indent = true;
      xml_format.IndentChars = ("\t");

      using (XmlWriter writer = XmlWriter.Create(filePath_helpers, xml_format))
      {
         writer.WriteStartDocument();
         writer.WriteStartElement("HelperObjSettings");

         foreach (var helper in CameraCtrl.Instance.helperPoints)
         {
            if (helper.Value != null)
            {
               writer.WriteStartElement(helper.Key);
               writer.WriteString(helper.Value.localPosition.ToString());
               writer.WriteEndElement();
            }
         }

         writer.WriteEndElement();
         writer.WriteEndDocument();
      }
   }

   private void ReadGlobalSettingsXML()
   {
      using (XmlReader reader = XmlReader.Create(filePath_global))
      {
         while (reader.Read())
         {
            // only detect start elements
            if (reader.IsStartElement())
            {
               switch (reader.Name)
               {
                  case "chaseRotationSpeed":
                     chaseRotationSpeed = reader.ReadElementContentAsFloat();
                     break;
                  case "zoomRotationSpeed":
                     zoomRotationSpeed = reader.ReadElementContentAsFloat();
                     break;
                  case "runRotationSpeed":
                     runRotationSpeed = reader.ReadElementContentAsFloat();
                     break;
                  case "invertY":
                     string str = reader.ReadElementContentAsString();
                     CameraCtrl.Instance.invertY = Convert.ToBoolean(str);
                     break;
               }
            }
         }

         CameraCtrl camCtrl = CameraCtrl.Instance;
         camCtrl.chaseRotationSpeed = chaseRotationSpeed;
         camCtrl.zoomRotationSpeed = zoomRotationSpeed;
         camCtrl.runRotationSpeed = runRotationSpeed;
      }
   }

   private void ReadHelpersXML()
   {
      using (XmlReader reader = XmlReader.Create(filePath_helpers))
      {
         while (reader.Read())
         {
            // only detect start elements
            if (reader.IsStartElement())
            {
               switch (reader.Name)
               {
                  case "FocusPoint":
                     focusPoint = CameraCtrl.Instance.helperPoints["FocusPoint"].gameObject;
                     focusPoint.transform.localPosition = UtilityScript.parseVector3(reader.ReadElementContentAsString());
                     CameraCtrl.Instance.helperPoints["FocusPoint"].gameObject.transform.localPosition = focusPoint.transform.localPosition;                     
                     break;
                  case "ZoomPoint":
                     zoomPoint = CameraCtrl.Instance.helperPoints["ZoomPoint"].gameObject;
                     zoomPoint.transform.localPosition = UtilityScript.parseVector3(reader.ReadElementContentAsString());
                     CameraCtrl.Instance.helperPoints["ZoomPoint"].gameObject.transform.localPosition = zoomPoint.transform.localPosition;
                     break;
                  case "ChasePoint":
                     chasePoint = CameraCtrl.Instance.helperPoints["ChasePoint"].gameObject;
                     chasePoint.transform.localPosition = UtilityScript.parseVector3(reader.ReadElementContentAsString());
                     CameraCtrl.Instance.helperPoints["ChasePoint"].gameObject.transform.localPosition = chasePoint.transform.localPosition;
                     break;
                  case "RunPoint":
                     runPoint = CameraCtrl.Instance.helperPoints["RunPoint"].gameObject;
                     runPoint.transform.localPosition = UtilityScript.parseVector3(reader.ReadElementContentAsString());
                     CameraCtrl.Instance.helperPoints["RunPoint"].gameObject.transform.localPosition = runPoint.transform.localPosition;
                     break;
               }
            }
         }
      }
   }
}