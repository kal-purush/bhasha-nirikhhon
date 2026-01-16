using System;
using System.Collections.Generic;
using System.Collections;
using System.IO;
using System.Threading;
using ColossalFramework;
using ColossalFramework.Math;
using ColossalFramework.UI;
using ICities;
using UnityEngine;

/// <summary>
/// Updated from SkylinesRoadUpdate added global preferences
/// added Additional selections
/// Added new to remove warning for reused m_
/// </summary>
namespace AnotherRoadUpdate
{
    public class RoadUpdateTool : DefaultTool
    {
        #region "Declarations"
        private enum Ups
        {
            Basic = 0,
            Highway = 1,
            Large = 2,
            Medium = 3,
            Oneway = 4
        }
        private enum p
        {
            Roads = 0,
            Railroads = 1,
            Highways = 2,
            PowerLines = 3,
            Pipes = 4,
            Buildings = 5,
            Trees = 6,
            Props = 7,
            Bridge = 8,
            Ground = 9,
            Tunnel = 10
        }

        private object m_dataLock = new object();
        
        private bool m_active;
        private bool m_oldLog;
        private bool m_selectable;
        private bool m_fromTypes;
        private bool m_toTypes;
        private bool m_fromRoads;
        private bool m_toRoads;
        private bool m_Updates;
        private bool m_settings;

        private new bool m_mouseRayValid;

        private string fromSelected = string.Empty;
        private string toSelected = string.Empty;

        private Vector3 m_startPosition;
        private Vector3 m_startDirection;
        private Vector3 m_mouseDirection;
        private Vector3 m_cameraDirection;
        private new Vector3 m_mousePosition;

        private new Ray m_mouseRay;
        private new float m_mouseRayLength;

        private List<ushort> segmentsToDelete;

        private float m_maxArea = 400f;

        private int m_topOfOptions;
        private int m_top = 0;
        private int m_left = 0;

        private UIButton mainButton;
        private UIPanel plMain;
        private UIPanel plOptions;
        private UIPanel plTypes;
        private UIPanel plRoads;
        private UIPanel plHighway;
        private UIPanel plLarge;
        private UIPanel plMedium;
        private UIPanel plOneway;
        private UIPanel plDelete;

        private UILabel lFrom;
        private UILabel lTo;
        private UILabel lOptions;
        private UILabel lLines;
        private UILabel lProperties;
        private UILabel lSelectable;

        private UICheckBox cbDelete;
        private UICheckBox cbUpdate;
        private UICheckBox cbServices;
        private UICheckBox cbToggle;        

        List<UICheckBox> toTypes = new List<UICheckBox>();
        List<UICheckBox> fromTypes = new List<UICheckBox>();

        List<UICheckBox> toRoads = new List<UICheckBox>();
        List<UICheckBox> fromRoads = new List<UICheckBox>();
        List<UICheckBox> delTypes = new List<UICheckBox>();

        private string[] m_options = new string[] { "Label Discription","Updates", "Delete", "Services", "Label Select" };
        private string[] m_roads = new string[] { "Basic Road Bicycle", "Basic Road Decoration Grass", "Basic Road Decoration Trees", "Basic Road Elevated Bike", "Basic Road Elevated Tram", "Basic Road Elevated", "Basic Road Slope", "Basic Road Tram", "Basic Road Tunnel Bike", "Basic Road Tunnel Tram", "Basic Road Tunnel", "Basic Road", "Gravel Road", "Highway Barrier", "Highway Barrier", "Highway Elevated", "Highway Tunnel", "Highway", "HighwayRamp Tunnel", "HighwayRamp", "HighwayRampElevated", "Large Oneway Decoration Grass", "Large Oneway Decoration Trees", "Large Oneway Elevated", "Large Oneway Road Tunnel", "Large Oneway", "Large Road Bicycle", "Large Road Bus", "Large Road Decoration Grass", "Large Road Decoration Trees", "Large Road Elevated Bike", "Large Road Elevated Bus", "Large Road Elevated", "Large Road Tunnel Bus", "Large Road Tunnel", "Large Road", "Medium Road Bicycle", "Medium Road Bus", "Medium Road Decoration Grass", "Medium Road Decoration Trees", "Medium Road Elevated Bike", "Medium Road Elevated Bus", "Medium Road Elevated Tram", "Medium Road Elevated", "Medium Road Tram", "Medium Road Tunnel Bus", "Medium Road Tunnel Tram", "Medium Road Tunnel", "Medium Road", "Oneway Road Decoration Grass", "Oneway Road Decoration Trees", "Oneway Road Elevated Tram", "Oneway Road Elevated", "Oneway Road Tram", "Oneway Road Tunnel Tram", "Oneway Road Tunnel", "Oneway Road", "Oneway Tram Track" };
        private string[] m_types = new string[] { "Basic", "Highway", "Large", "Medium", "Oneway" };
        private string[] m_deletes = new string[] { "Label Lines", "Roads", "Railroads", "Highways", "PowerLines", "Pipes", "Label Properties", "Buildings", "Trees", "Props", "Label Options", "Bridge", "Ground", "Tunnel" };

        UserSettings us = new UserSettings();

        #endregion

        #region "Public Procedures"

        protected override void Awake()
        {
            WriteLog("ARUT awake!", true);
            if (m_oldLog == false)
            {
                m_oldLog = true;
            }
            m_active = false;
            base.Awake();
        }

        protected override void OnEnable()
        {
            UIView.GetAView().FindUIComponent<UITabstrip>("MainToolstrip").selectedIndex = -1;
            base.OnEnable();
        }

        protected override void OnDisable()
        {
            WriteLog("ARUT sleep!");
            if (plMain != null)
                plMain.isVisible = false;
            SetSettings();
            base.OnDisable();
        }

        protected override void OnDestroy()
        {
            SetSettings();
            base.OnDestroy();
        }

        protected override void OnToolGUI(Event e)
        {
            if (e.type == EventType.MouseDown && m_mouseRayValid && m_selectable)
            {
                if (e.button == 0)
                {
                    m_active = true;
                    this.m_startPosition = this.m_mousePosition;
                    this.m_startDirection = Vector3.forward;
                }
                if (e.button == 1)
                {
                    m_active = false;
                }
            }
            else if (e.type == EventType.MouseUp && m_active)
            {
                if (e.button == 0)
                {
                    //handle Updates
                    ApplyUpdates();
                    //Handle Deletes
                    ApplyBulldoze();
                    //Handle Services on/off
                    ApplyServices();
                    m_active = false;
                }
            }
        }

        /// <summary>
        /// The GUI initial state procedure
        /// Will load all form (panel) objects
        /// </summary>
        /// <param name="mode"></param>
        public void InitGui(LoadMode mode)
        {
            WriteLog("Entering InitGUI");

            mainButton = UIView.GetAView().FindUIComponent<UIButton>("MarqueeBulldozer");

            if (mainButton == null)
            {
                var RoadUpdateButton = UIView.GetAView().FindUIComponent<UIMultiStateButton>("BulldozerButton");

                mainButton = RoadUpdateButton.parent.AddUIComponent<UIButton>();
                mainButton.name = "RoadUpdateButton";
                mainButton.size = new Vector2(40, 40);
                mainButton.tooltip = "Another Road Update tool";
                mainButton.relativePosition = new Vector2
                (
                    RoadUpdateButton.relativePosition.x + RoadUpdateButton.width / 2.0f - (mainButton.width) - RoadUpdateButton.width,
                    RoadUpdateButton.relativePosition.y + RoadUpdateButton.height / 2.0f - mainButton.height / 2.0f
                );

                mainButton.normalFgSprite = RoadUpdateButton.normalFgSprite;
                mainButton.focusedFgSprite = RoadUpdateButton.focusedFgSprite;
                mainButton.hoveredFgSprite = RoadUpdateButton.hoveredFgSprite;

                mainButton.eventClick += Button_Clicked;

                plMain = UIView.GetAView().FindUIComponent("TSBar").AddUIComponent<UIPanel>();

                if (mode == LoadMode.LoadMap || mode == LoadMode.NewMap)
                    plMain.backgroundSprite = "GenericPanel";
                else
                    plMain.backgroundSprite = "SubcategoriesPanel";

                plMain.isVisible = false;
                plMain.name = "MarqueeBulldozerSettings";
                int left = 10;
                int right = 280;
                int top = 1;
                int loc = 0;

                try
                {
                    //create our main view
                    addUILabel(plMain, top, 1, "Select your update type, Delete, Update, Toggle", true);
                    top += 25;
                    //Updates
                    cbUpdate = addCheckbox(plMain, top, left, "Update", "Select to display the Update options", true);
                    cbUpdate.eventCheckChanged += cbUpdate_eventCheckChanged;
                    //Bulldoze
                    cbDelete = addCheckbox(plMain, top, left + 90, "Delete", "Select to display the BullDoze options", true);
                    cbDelete.eventCheckChanged += cbUpdate_eventCheckChanged;
                    //Services Toggle
                    cbServices = addCheckbox(plMain, top, left + 180, "Services", "Select to display the Services on and off options", true);
                    cbServices.eventCheckChanged += cbUpdate_eventCheckChanged;
                    //divider
                    top += 15;
                    addUILabel(plMain, top, 5, "____________________________________________________________________", true);
                    top += 20;

                    m_top = top;
                    m_left = left;

                    //Updates labels
                    lFrom = addUILabel(plMain, top, 1, "From ==>", false);
                    lTo = addUILabel(plMain, top, right, "To ==>", false);
                    top += 25;

                    loc += 1;
                    int cb = 0;
                    int x = top;
                    //load the update types but hide them
                    foreach (string s in m_types)
                    {
                        string t = String.Format("If checked {0} sections will be displayed.", s);
                        fromTypes.Add(addCheckbox(plMain, x, left, s, t, false));
                        toTypes.Add(addCheckbox(plMain, x, right, s, t, false));
                        fromTypes[cb].eventCheckChanged += cbBasic_eventCheckChanged;
                        toTypes[cb].eventCheckChanged += cbToBasic_eventCheckChanged;
                        x += 25;
                        cb++;
                    }

                    loc += 1;
                    cb = 0;
                    //load the update roads but hide them
                    foreach (string s in m_roads)
                    {
                        string t = String.Format("If checked {0} sections will be converted.", s);
                        fromRoads.Add(addCheckbox(plMain, x, left * 2, s, t, false));
                        t = String.Format("If checked sections will convert to {0}.", s);
                        toRoads.Add(addCheckbox(plMain, x, right + 20, s, t, false));
                        x += 25;
                        fromRoads[cb].eventCheckChanged += FromRoad_eventCheckChanged;
                        toRoads[cb].eventCheckChanged += ToRoad_eventCheckChanged;
                        cb++;
                    }

                    //load the bulldoze options but hide them (these are just options)
                    loc += 1;
                    cb = 0;
                    int temp = top;
                    foreach (string s in m_deletes)
                    {
                        if (s == "Lines") {
                            lLines = addUILabel(plMain, temp - 3, left, s, false);
                            temp += 25;
                        }
                        else if (s == "Properties") {
                            lProperties = addUILabel(plMain, temp - 3, left, s, false);
                            temp += 25;
                        }
                        else if (s == "Options") {
                            lOptions = addUILabel(plMain, temp - 3, left, s, false);
                            temp += 25;
                        }
                        else
                        {
                            delTypes.Add(addCheckbox(plMain, temp, left, s, "", false));
                            //WriteLog("Deltype: " + delTypes[cb] + " ::text: " + delTypes[cb].text + " :: cb: " + cb);
                            delTypes[cb].eventCheckChanged += DeleteTypes_eventCheckChanged;
                            temp += 25;
                            cb += 1;
                        }
                    }

                    //add a option for turning services on and off
                    cbToggle = addCheckbox(plMain, top, left, "Toggle Services On/Off", "Services 'On' or 'Off'", false);

                }
                catch (Exception ex)
                {
                  WriteLog("Error in GUIInit: " + ex.Message + " loc = " + loc);
                }

                //add space for 6 options
                top += (25 * 5);
                m_topOfOptions = top;
                //add space for 15 sub options
                top += (25 * 15);

                //add events to update the settings
                plMain.size = new Vector2(575, top + 25);

                //let the user know thaey can select areas to update
                lSelectable = addUILabel(plMain, 1, 575 - 160, "Select enabled", false);

                //load last used values
                GetSettings(true);
                plMain.relativePosition = new Vector2
                (
                    RoadUpdateButton.relativePosition.x + RoadUpdateButton.width / 2.0f - plMain.width,
                    RoadUpdateButton.relativePosition.y - plMain.height
                );
              WriteLog("Leaving InitGUI");
            }
        }

        private void DeleteTypes_eventCheckChanged(UIComponent component, bool value)
        {
          WriteLog("Entering DeleteTypes_eventCheckChanged");
            UICheckBox c = (UICheckBox)component;
            //store the update
            if (c.text == delTypes[(int)p.Pipes].text) { us.Pipes = value; }
            if (c.text == delTypes[(int)p.Ground].text) { us.Ground = value; }
            if (c.text == delTypes[(int)p.Tunnel].text) { us.Tunnel = value; }
            if (c.text == delTypes[(int)p.Bridge].text) { us.Bridge = value; }
            if (c.text == delTypes[(int)p.Props].text) { us.Props = value; }
            if (c.text == delTypes[(int)p.PowerLines].text) { us.PowerLines = value; }
            if (c.text == delTypes[(int)p.Trees].text) { us.Trees = value; }
            if (c.text == delTypes[(int)p.Buildings].text) { us.Buildings = value; }
            if (c.text == delTypes[(int)p.Highways].text) { us.Highways = value; }
            if (c.text == delTypes[(int)p.Railroads].text) { us.Railroads = value; }
            if (c.text == delTypes[(int)p.Roads].text) { us.Roads = value; }
            //set enabled
            SetDeleteEnabled();
          WriteLog("Leaving DeleteTypes_eventCheckChanged");
        }

        private void SetDeleteEnabled()
        {
          WriteLog("Entering SetDeleteEnabled selectable is: " + m_selectable);
            try
            {
                m_selectable = ((delTypes[(int)p.Ground].isChecked || 
                                    delTypes[(int)p.Bridge].isChecked || 
                                    delTypes[(int)p.Tunnel].isChecked) && 
                                (delTypes[(int)p.Pipes].isChecked || 
                                    delTypes[(int)p.Props].isChecked || 
                                    delTypes[(int)p.PowerLines].isChecked || 
                                    delTypes[(int)p.Trees].isChecked || 
                                    delTypes[(int)p.Railroads].isChecked || 
                                    delTypes[(int)p.Roads].isChecked));

                lSelectable.isVisible = m_selectable;
            }
            catch (Exception ex)
            {
              WriteLog("SetDeleteEnabled Exception: " + ex.Message);
            }
          WriteLog("Leaving SetDeleteEnabled selectable is: " + m_selectable);
        }

        protected override void OnToolLateUpdate()
        {
            Vector3 mousePosition = Input.mousePosition;
            Vector3 cameraDirection = Vector3.Cross(Camera.main.transform.right, Vector3.up);
            cameraDirection.Normalize();
            while (!Monitor.TryEnter(this.m_dataLock, SimulationManager.SYNCHRONIZE_TIMEOUT))
            {
            }
            try
            {
                this.m_mouseRay = Camera.main.ScreenPointToRay(mousePosition);
                this.m_mouseRayLength = Camera.main.farClipPlane;
                this.m_cameraDirection = cameraDirection;
                this.m_mouseRayValid = (!this.m_toolController.IsInsideUI && UnityEngine.Cursor.visible);
            }
            finally
            {
                Monitor.Exit(this.m_dataLock);
            }
        }

        private void SetSettings()
        {
            if (m_settings == true) { return; }
            m_settings = true;

            WriteLog("Entering SetSetting cbDelete.isChecked: " + cbDelete.isChecked);

            us.ToOneway = toTypes[(int)Ups.Oneway].isChecked;
            us.ToMedium = toTypes[(int)Ups.Medium].isChecked;
            us.ToHighway = toTypes[(int)Ups.Highway].isChecked;
            us.ToLarge = toTypes[(int)Ups.Large].isChecked;
            us.ToBasic = toTypes[(int)Ups.Basic].isChecked;
            us.Oneway = fromTypes[(int)Ups.Oneway].isChecked;
            us.Medium = fromTypes[(int)Ups.Medium].isChecked;
            us.Highway = fromTypes[(int)Ups.Highway].isChecked;
            us.Large = fromTypes[(int)Ups.Large].isChecked;
            us.Basic = fromTypes[(int)Ups.Basic].isChecked;

            us.Roads = delTypes[(int)p.Roads].isChecked;
            us.Railroads = delTypes[(int)p.Railroads].isChecked;
            us.Highways = delTypes[(int)p.Highways].isChecked;
            us.PowerLines = delTypes[(int)p.PowerLines].isChecked;
            us.Pipes = delTypes[(int)p.Pipes].isChecked;
            us.Props = delTypes[(int)p.Props].isChecked;
            us.Buildings = delTypes[(int)p.Buildings].isChecked;
            us.Trees = delTypes[(int)p.Trees].isChecked;
            us.Ground = delTypes[(int)p.Ground].isChecked;
            us.Bridge = delTypes[(int)p.Bridge].isChecked;
            us.Tunnel = delTypes[(int)p.Tunnel].isChecked;

            us.Services = cbServices.isChecked;
            us.Delete = cbDelete.isChecked;
            us.Update = cbUpdate.isChecked;
            us.Toggle = cbToggle.isChecked;

            us.Save();

            WriteLog("Leaving SetSettings cbDelete.isChecked: " + cbDelete.isChecked);
            m_settings = false;
        }

        private void GetSettings(bool toroads)
        {
            if (m_settings == true) { return; }
            m_settings = true;

            int loc = 0;
            WriteLog("Entering GetSettings cbDelete.isChecked: " + cbDelete.isChecked);
            try
            {
                toTypes[(int)Ups.Oneway].isChecked = us.ToOneway;
                toTypes[(int)Ups.Medium].isChecked = us.ToMedium;
                toTypes[(int)Ups.Highway].isChecked = us.ToHighway;
                toTypes[(int)Ups.Large].isChecked = us.ToLarge;
                toTypes[(int)Ups.Basic].isChecked = us.ToBasic;
                fromTypes[(int)Ups.Oneway].isChecked = us.Oneway;
                fromTypes[(int)Ups.Medium].isChecked = us.Medium;
                fromTypes[(int)Ups.Highway].isChecked = us.Highway;
                fromTypes[(int)Ups.Large].isChecked = us.Large;
                fromTypes[(int)Ups.Basic].isChecked = us.Basic;
                delTypes[(int)p.Roads].isChecked = us.Roads;
                delTypes[(int)p.Railroads].isChecked = us.Railroads;
                delTypes[(int)p.Highways].isChecked = us.Highways;
                delTypes[(int)p.PowerLines].isChecked = us.PowerLines;
                delTypes[(int)p.Pipes].isChecked = us.Pipes;
                delTypes[(int)p.Buildings].isChecked = us.Buildings;
                delTypes[(int)p.Trees].isChecked = us.Trees;
                delTypes[(int)p.Props].isChecked = us.Props;
                delTypes[(int)p.Bridge].isChecked = us.Bridge;
                delTypes[(int)p.Ground].isChecked = us.Ground;
                delTypes[(int)p.Tunnel].isChecked = us.Tunnel;
                cbServices.isChecked = us.Services;
                cbDelete.isChecked = us.Delete;
                loc += 1;
                cbUpdate.isChecked = us.Update;
                cbToggle.isChecked = us.Toggle;
            }
            catch (Exception ex)
            {
                WriteLog("GetSettings loc: " + loc + " deltypes.Count: " + delTypes.Count + " Exception: " + ex.Message);
            }
            WriteLog("Leaving GetSettings cbDelete.isChecked: " + cbDelete.isChecked);
            m_settings = false;
        }

        #endregion

        #region "Event Handlers"

        void Button_Clicked(UIComponent component, UIMouseEventParameter eventParam)
        {
            if (plMain.isVisible == true)
            {
                this.enabled = false;
                plMain.isVisible = false;
            }
            else
            {
                this.enabled = true;
                plMain.isVisible = true;
            }
        }

        private void cbUpdate_eventCheckChanged(UIComponent component, bool value)
        {
            if (m_Updates == true) { return; }
            m_Updates = true;

            int loc = 0;
            WriteLog("Entering: cbUpdate_eventCheckChanged");
            try
            {
                //Hide them all
                foreach (UICheckBox c in fromTypes) { c.isVisible = false; c.label.isVisible = false; }
                foreach (UICheckBox c in toTypes) { c.isVisible = false; c.label.isVisible = false; }
                foreach (UICheckBox c in fromRoads) { c.isVisible = false; c.label.isVisible = false; }
                foreach (UICheckBox c in toRoads) { c.isVisible = false; c.label.isVisible = false; }
                foreach (UICheckBox c in delTypes) { c.isVisible = false; c.label.isVisible = false; }

                cbToggle.isChecked = false;
                WriteLog("cbUpdate_eventCheckChanged ShowDelete");
                ShowDelete(false);
                WriteLog("cbUpdate_eventCheckChanged ShowToggle");
                ShowToggle(false);
                WriteLog("cbUpdate_eventCheckChanged ShowUpdate");
                ShowUpdate(false);

                loc += 1;
                UICheckBox cb = (UICheckBox)component;
                if (cb.text == "Update" && cbUpdate.isChecked == true)
                {
                    cbDelete.isChecked = false;
                    cbServices.isChecked = false;
                    ShowUpdate(true);
                }
                else if (cb.text == "Delete" && cbDelete.isChecked == true)
                {
                    cbUpdate.isChecked = false;
                    cbServices.isChecked = false;
                    ShowDelete(true);
                }
                else if (cb.text == "Services" && cbServices.isChecked == true)
                {
                    cbDelete.isChecked = false;
                    cbUpdate.isChecked = false;
                    ShowToggle(true);
                }
                loc += 1;
                SetSettings();
            }
            catch (Exception ex)
            {
                WriteLog("Exception in cbUpdate_eventCheckChanged loc: " + loc + " Message: " + ex.Message + "::" + ex.StackTrace);
            }
            m_Updates = false;
            WriteLog("Leaving cbUpdate_eventCheckChanged");
        }

        private void cbBasic_eventCheckChanged(UIComponent component, bool value)
        {
            if (m_fromTypes == true) { return; }
            m_fromTypes = true;

            WriteLog("Entering cbBasic_eventCheckChanged");

            UICheckBox cb = (UICheckBox)component;
            UpdateDisplayedRoads(fromTypes, fromRoads, cb.text, value, 20);
            SetSettings();
            m_fromTypes = false;

            WriteLog("Leaving cbBasic_eventCheckChanged");
        }

        private void cbToBasic_eventCheckChanged(UIComponent component, bool value)
        {
            if (m_toTypes == true) { return; }
            m_toTypes = true;

            WriteLog("Entering cbToBasic_eventCheckChanged");

            UICheckBox cb = (UICheckBox)component;
            UpdateDisplayedRoads(toTypes, toRoads, cb.text, value, 300);
            SetSettings();
            m_toTypes = false;

            WriteLog("Leaving cbToBasic_eventCheckChanged");
        }

        private void ToRoad_eventCheckChanged(UIComponent component, bool value)
        {
            if (m_toRoads == true) { return; }
            if (cbUpdate.isChecked == false) { return; }
            m_toRoads = true;

            WriteLog("Entering ToRoad_eventCheckedChanged");

            UICheckBox cb = (UICheckBox)component;

            UpdateSelected(toRoads, lTo, cb, cb.isChecked, "To ==> ", out toSelected);
            SetSettings();
            m_toRoads = false;

            WriteLog("Leaving ToRoad_eventCheckedChanged");
        }

        private void FromRoad_eventCheckChanged(UIComponent component, bool value)
        {
            if (m_fromRoads == true) { return; }
            if (cbUpdate.isChecked == false) { return; }
            m_fromRoads = true;

            WriteLog("Entering FromRoad_eventCheckedChanged");

            UICheckBox cb = (UICheckBox)component;

            UpdateSelected(fromRoads, lFrom, cb, cb.isChecked, "From ==> ", out fromSelected);
            SetSettings();
            m_fromRoads = false;

            WriteLog("Leaving FromRoad_eventCheckedChanged");
        }

        #endregion

        #region "Private procedures"

        private void ShowToggle(bool show)
        {
            cbToggle.isVisible = show;
            cbToggle.label.isVisible = show;
        }

        private void ShowDelete(bool show)
        {
            try
            {
                lLines.isVisible = show;
                lProperties.isVisible = show;
                lOptions.isVisible = show;
                foreach (UICheckBox c in delTypes)
                {
                    c.isVisible = show;
                    c.label.isVisible = show;
                }
            }
            catch (Exception ex)
            {
              WriteLog("Exception in ShowDelete: " + ex.Message);
            }
        }

        private void ShowUpdate(bool show)
        {
            lFrom.isVisible = show;
            lTo.isVisible = show;

            //foreach (UICheckBox cb in fromTypes)
            //{
            //    cb.isVisible = show;
            //    cb.label.isVisible = show;
            //    if (cb.isChecked == true && cbUpdate.isChecked)
            //    {
            //        UpdateDisplayedRoads(fromTypes, fromRoads, cb.text, true, 20);
            //    }
            //}
            //foreach (UICheckBox cb in toTypes)
            //{
            //    cb.isVisible = show;
            //    cb.label.isVisible = show;
            //    if (cb.isChecked == true && cbUpdate.isChecked)
            //    {
            //        UpdateDisplayedRoads(toTypes, toRoads, cb.text, true, 300);
            //    }
            //}
        }

        private void UpdateDisplayedRoads(List<UICheckBox> types, List<UICheckBox> roads, string text, bool show, int xPos)
        {
            WriteLog("Entering UpdateDisplayedRoads " + xPos);

            foreach (UICheckBox cb in types)
            {
                if (cb.text != text)
                {
                    cb.isChecked = false;
                }
            }

            if (text.Contains("Basic")) { DisplayCheckBoxes(roads, xPos, "Basic", show); }
            else if (text.Contains("Highway")) { DisplayCheckBoxes(roads, xPos, "Highway", show); }
            else if (text.Contains("Large")) { DisplayCheckBoxes(roads, xPos, "Large", show); }
            else if (text.Contains("Medium")) { DisplayCheckBoxes(roads, xPos, "Medium", show); }
            else if (text.Contains("Oneway")) { DisplayCheckBoxes(roads, xPos, "Oneway", show); }
            else
                WriteLog("Could not define this object type.");

            WriteLog("Leaving UpdateDisplayedRoads");
        }

        private void DisplayCheckBoxes(List<UICheckBox> roads, int xPos, string test, bool show)
        {
            WriteLog("Entering DisplayCheckBoxes");

            int y = m_topOfOptions;
            int x = xPos;

            foreach (UICheckBox cb in roads)
            {
                cb.isVisible = false;
                cb.label.isVisible = false;
                if (cb.text.Contains(test))
                {
                    cb.relativePosition = new Vector3(x, y);
                    cb.label.relativePosition = new Vector3(x + 25, y + 3);
                    cb.isVisible = show;
                    cb.label.isVisible = show;
                    y += 25;
                }
            }

            WriteLog("Leaving UpdateDisplayedRoads");
        }

        private void UpdateSelected(List<UICheckBox> roads, UILabel lb, UICheckBox incb, bool check, string tofrom, out string selected)
        {
            WriteLog("Entering UpdateSelected: " + tofrom);

            selected = "";
            if (plMain.isVisible == false) return;
            lb.text = tofrom;

            foreach (UICheckBox cb in roads)
            {
                if (cb.text != incb.text && cb.isChecked == true)
                {
                    cb.isChecked = false;
                }
            }
            if (incb.isChecked)
            {
                selected = incb.text;
                lb.text = tofrom + selected.Replace("Decoration ", "Dec.. ");
            }
            //Set to allow area selections
            m_selectable = (fromSelected != "" && toSelected != "");

            WriteLog("Leaving UpdateSelected: " + tofrom);
        }

        #region "Update Add Controls"

        private UILabel addUILabel(UIPanel panel, int yPos, int xPos, string text, bool hidden)
        {
            UILabel lb = panel.AddUIComponent<UILabel>();
            lb.relativePosition = new Vector3(xPos, yPos);
            lb.height = 0;
            lb.width = 80;
            lb.text = text;
            lb.isVisible = hidden;
            return lb;
        }

        private UICheckBox addCheckbox(UIPanel panel, int yPos, int xPos, string text, string tooltip, bool hidden)
        {
            var cb = plMain.AddUIComponent<UICheckBox>();
            cb.relativePosition = new Vector3(xPos, yPos);
            cb.height = 0;
            cb.width = 80;
  
            var label = plMain.AddUIComponent<UILabel>();
            label.relativePosition = new Vector3(xPos + 25, yPos + 3);
            cb.label = label;
            cb.text = text;

            UISprite uncheckSprite = cb.AddUIComponent<UISprite>();
            uncheckSprite.height = 20;
            uncheckSprite.width = 20;
            uncheckSprite.relativePosition = new Vector3(0, 0);
            uncheckSprite.spriteName = "check-unchecked";

            UISprite checkSprite = cb.AddUIComponent<UISprite>();
            checkSprite.height = 20;
            checkSprite.width = 20;
            checkSprite.relativePosition = new Vector3(0, 0);
            checkSprite.spriteName = "check-checked";

            cb.checkedBoxObject = checkSprite;

            cb.tooltip = tooltip;
            label.tooltip = cb.tooltip;
            cb.isChecked = false;
            cb.label.isVisible = hidden;
            cb.isVisible = hidden;

            return cb;
        }

        #endregion

        #region "Update Area Selection"

        public override void RenderOverlay(RenderManager.CameraInfo cameraInfo)
        {
            while (!Monitor.TryEnter(this.m_dataLock, SimulationManager.SYNCHRONIZE_TIMEOUT))
            {
            }
            Vector3 startPosition;
            Vector3 mousePosition;
            Vector3 startDirection;
            Vector3 mouseDirection;
            bool active;

            try
            {
                active = this.m_active;

                startPosition = this.m_startPosition;
                mousePosition = this.m_mousePosition;
                startDirection = this.m_startDirection;
                mouseDirection = this.m_mouseDirection;
            }
            finally
            {
                Monitor.Exit(this.m_dataLock);
            }

            var color = Color.red;

            if (!active)
            {
                base.RenderOverlay(cameraInfo);
                return;
            }

            Vector3 a = (!active) ? mousePosition : startPosition;
            Vector3 vector = mousePosition;
            Vector3 a2 = (!active) ? mouseDirection : startDirection;
            Vector3 a3 = new Vector3(a2.z, 0f, -a2.x);

            float num = Mathf.Round(((vector.x - a.x) * a2.x + (vector.z - a.z) * a2.z) * 0.125f) * 8f;
            float num2 = Mathf.Round(((vector.x - a.x) * a3.x + (vector.z - a.z) * a3.z) * 0.125f) * 8f;

            float num3 = (num < 0f) ? -4f : 4f;
            float num4 = (num2 < 0f) ? -4f : 4f;

            Quad3 quad = default(Quad3);
            quad.a = a - a2 * num3 - a3 * num4;
            quad.b = a - a2 * num3 + a3 * (num2 + num4);
            quad.c = a + a2 * (num + num3) + a3 * (num2 + num4);
            quad.d = a + a2 * (num + num3) - a3 * num4;

            if (num3 != num4)
            {
                Vector3 b = quad.b;
                quad.b = quad.d;
                quad.d = b;
            }
            ToolManager toolManager = ToolManager.instance;
            toolManager.m_drawCallData.m_overlayCalls++;
            RenderManager.instance.OverlayEffect.DrawQuad(cameraInfo, color, quad, -1f, 1025f, false, true);
            base.RenderOverlay(cameraInfo);
            return;
        }

        /// <summary>
        /// Updated to set max range
        /// </summary>
        /// <param name="newMousePosition"></param>
        /// <returns></returns>
        private bool checkMaxArea(Vector3 newMousePosition)
        {
            if ((m_startPosition -newMousePosition).sqrMagnitude > m_maxArea * 100000)
            {
                return false;
            }
            return true;
        }

        public override void SimulationStep()
        {
            while (!Monitor.TryEnter(this.m_dataLock, SimulationManager.SYNCHRONIZE_TIMEOUT))
            {
            }
            Ray mouseRay;
            Vector3 cameraDirection;
            bool mouseRayValid;
            try
            {
                mouseRay = this.m_mouseRay;
                cameraDirection = this.m_cameraDirection;
                mouseRayValid = this.m_mouseRayValid;
            }
            finally
            {
                Monitor.Exit(this.m_dataLock);
            }

            ToolBase.RaycastInput input = new ToolBase.RaycastInput(mouseRay, m_mouseRayLength);
            ToolBase.RaycastOutput raycastOutput;
            if (mouseRayValid && ToolBase.RayCast(input, out raycastOutput))
            {
                if (!m_active)
                {
                    while (!Monitor.TryEnter(this.m_dataLock, SimulationManager.SYNCHRONIZE_TIMEOUT))
                    {
                    }
                    try
                    {
                        this.m_mouseDirection = cameraDirection;
                        this.m_mousePosition = raycastOutput.m_hitPos;
                    }
                    finally
                    {
                        Monitor.Exit(this.m_dataLock);
                    }
                }
                else
                {
                    while (!Monitor.TryEnter(this.m_dataLock, SimulationManager.SYNCHRONIZE_TIMEOUT))
                    {
                    }
                    try
                    {
                        if (checkMaxArea(raycastOutput.m_hitPos))
                        {
                            this.m_mousePosition = raycastOutput.m_hitPos;
                        }
                    }
                    finally
                    {
                        Monitor.Exit(this.m_dataLock);
                    }
                }

            }
        }
        
        int RebuildSegment(int segmentIndex, NetInfo newPrefab, bool roadDirectionMatters, Vector3 directionPoint, Vector3 direction, ref ToolBase.ToolErrors error)
        {
            NetManager net = Singleton<NetManager>.instance;

            if (segmentIndex >= net.m_segments.m_buffer.Length)
                return 0;

            NetInfo prefab = net.m_segments.m_buffer[segmentIndex].Info;

            NetTool.ControlPoint startPoint;
            NetTool.ControlPoint middlePoint;
            NetTool.ControlPoint endPoint;
            GetSegmentControlPoints(segmentIndex, out startPoint, out middlePoint, out endPoint);
            
            bool test = false;
            bool visualize = false;
            bool autoFix = true;
            bool needMoney = false;
            bool invert = false;
            
            ushort node = 0;
            ushort segment = 0;
            int cost = 0;
            int productionRate = 0;

            NetTool.CreateNode(newPrefab, startPoint, middlePoint, endPoint, NetTool.m_nodePositionsSimulation, 1000, test, visualize, autoFix, needMoney, invert, false, (ushort)0, out node, out segment, out cost, out productionRate);
            
            if (segment != 0)
            {
                if (newPrefab.m_class.m_service == ItemClass.Service.Road)
                {
                    Singleton<CoverageManager>.instance.CoverageUpdated(ItemClass.Service.None, ItemClass.SubService.None, ItemClass.Level.None);
                }
                
                return segment;
            }

            return 0;
        }

        #endregion

        #region "Updates, Deletes, Toggles, Oh my!"

        private int ConvertObjects(string convertTo, string convertFrom, bool test, out int totalCost, out ToolBase.ToolErrors errors)
        {
            int num = 0;
            totalCost = 0;
            errors = 0;

            StringWriter sw = new StringWriter();
            //sw.WriteLine(String.Format("Entering ConvertObjects at {0}.", DateTime.Now.TimeOfDay

            NetInfo info = PrefabCollection<NetInfo>.FindLoaded(convertTo);

            if (info == null)
            {
                sw.WriteLine("Could not find the object: " + convertTo + ", aborting.");
                return num;
            }
         
            NetSegment[] buffer = Singleton<NetManager>.instance.m_segments.m_buffer;

            //sw.WriteLine("Filling Singleton<NetManager>.instance.m_segments.m_buffer. Found: " + buffer.Length);

            for (int i = 0; i < buffer.Length - 1; i++)
            {
                NetSegment segment = buffer[i];

                // do not uncomment unless you know not to multi click
                //if (Array.Exists(m_roads, (s) => { return s == segment.Info.name; }))
                //{
                //    sw.WriteLine(segment.Info.name + " from: " + convertFrom + " to: " + convertTo);
                //}
                //sw.WriteLine(segment.Info.name + " from: " + convertFrom + " to: " + convertTo);

                //Validate in selected area
                if (segment.Info == null)
                {
                    //sw.WriteLine(String.Format("Segment {0} is Null.",segment.Info.name
                }
                else if (segment.Info.name != convertFrom)
                {
                    //if (segment.Info.name != "Pedestrian Gravel")
                    //    sw.WriteLine(String.Format("Segment {0} is not a converting item.", segment.Info.name
                }
                else if (ValidateSelectedArea(segment) == false)
                {
                    //sw.WriteLine(String.Format("Segment {0} is not in the selected area.", segment.Info.name
                }
                else
                {
                    try
                    {
                        NetTool.ControlPoint point;
                        NetTool.ControlPoint point2;
                        NetTool.ControlPoint point3;

                        //sw.WriteLine(segment.Info.name + " converting to " + convertTo + ".");
                        //sw.WriteLine("Segment is a corner: " + segment.m_cornerAngleStart + " X " + segment.m_cornerAngleEnd);
                        //sw.WriteLine("About to call GetSegmentControlPoints.\n");

                        this.GetSegmentControlPoints(i, out point, out point2, out point3);
                        bool flag = false;
                        bool flag2 = true;
                        bool flag3 = true;
                        bool flag4 = false;
                        ushort num3 = 0;
                        ushort num4 = 0;
                        int num5 = 0;
                        int num6 = 0;

                        //test for bad index
                        if ((point.m_position == new Vector3()) && (point.m_position == new Vector3()) && (point.m_position == new Vector3())) { }
                            else
                        {
                            //sw.WriteLine("About to call NetTool.Create test mode.\n");
                            //Validate in area and other errors (no money!)
                            errors = NetTool.CreateNode(info, point, point2, point3, NetTool.m_nodePositionsSimulation, 0x3e8, true, flag, flag2, flag3, flag4, false, 0, out num3, out num4, out num5, out num6);

                            //sw.WriteLine("Called NetTool.Create.\n");
                            if (errors == 0)
                            {
                                errors = NetTool.CreateNode(info, point, point2, point3, NetTool.m_nodePositionsSimulation, 0x3e8, false, flag, flag2, flag3, flag4, false, 0, out num3, out num4, out num5, out num6);
                                num++;
                                totalCost += num5;
                            }
                            else if (test == false)
                            {
                                sw.WriteLine("Could not convert: " + segment.Info.name + " to " + convertTo + ". Message: " + GetMessage(errors));
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        sw.WriteLine("Error converting: " + segment.Info.name + " to " + convertTo + ". Message: " + ex.Message);
                    }
                }
            }
            //sw.WriteLine("Exiting ConvertObjects. num = " + num);
            //sw.WriteLine(String.Format("Exiting ConvertObjects at: {0} after converting: {1} items .", DateTime.Now.TimeOfDay, num
          WriteLog("" + sw);
            UIView.RefreshAll(true);
            return num;
        }

        private void ToggleServices(bool toggle, ItemClass.Service service)
        {
            FastList<ushort> fl = Singleton<BuildingManager>.instance.GetServiceBuildings(service);
            
            Building[] buffer =  Singleton<BuildingManager>.instance.m_buildings.m_buffer;
            
            for (int i = 0; i < buffer.Length; i++)
            {
                if (buffer[i].Info.GetService() == service)
                {
                    Building bi = buffer[i];
                    string bn = bi.Info.gameObject.name;

                    Vector3 v3 = bi.CalculatePosition(new Vector3());

                  WriteLog("Settings before: " + bi.Info.GetAI().enabled, true);

                    //bi.Info.gameObject.SetActive(toggle);
                    //bi.Info.enabled = toggle;
                    bi.Info.GetAI().enabled = toggle;
                    //bi.m_flags.SetFlags(Building.Flags.Active, false);

                  WriteLog("Settings after: " + bi.Info.GetAI().enabled);
                }
            }
        }

        protected void BulldozeRoads()
        {
            segmentsToDelete = new List<ushort>();

            var minX = this.m_startPosition.x < this.m_mousePosition.x ? this.m_startPosition.x : this.m_mousePosition.x;
            var minZ = this.m_startPosition.z < this.m_mousePosition.z ? this.m_startPosition.z : this.m_mousePosition.z;
            var maxX = this.m_startPosition.x > this.m_mousePosition.x ? this.m_startPosition.x : this.m_mousePosition.x;
            var maxZ = this.m_startPosition.z > this.m_mousePosition.z ? this.m_startPosition.z : this.m_mousePosition.z;

            int gridMinX = Mathf.Max((int)((minX - 16f) / 64f + 135f), 0);
            int gridMinZ = Mathf.Max((int)((minZ - 16f) / 64f + 135f), 0);
            int gridMaxX = Mathf.Min((int)((maxX + 16f) / 64f + 135f), 269);
            int gridMaxZ = Mathf.Min((int)((maxZ + 16f) / 64f + 135f), 269);

            for (int i = gridMinZ; i <= gridMaxZ; i++)
            {
                for (int j = gridMinX; j <= gridMaxX; j++)
                {
                    ushort num5 = NetManager.instance.m_segmentGrid[i * 270 + j];
                    int num6 = 0;
                    bool skip = false;
                    while (num5 != 0u)
                    {
                        var segment = NetManager.instance.m_segments.m_buffer[(int)((UIntPtr)num5)];

                        Vector3 position = segment.m_middlePosition;
                        float positionDiff = Mathf.Max(Mathf.Max(minX - 16f - position.x, minZ - 16f - position.z), Mathf.Max(position.x - maxX - 16f, position.z - maxZ - 16f));

                        if (positionDiff < 0f && segment.Info.name != "Airplane Path" && segment.Info.name != "Ship Path")
                        {
                            string seg = segment.Info.name;
                            // we need to handle Bridges (Elevated, Slope), tunnels, railroads, Pipe, Power Lines, 
                            if (delTypes[(int)p.Tunnel].isChecked == false && (seg.Contains("Tunnel") || seg.Contains("Slope")))
                                skip = true;
                            else if (delTypes[(int)p.Bridge].isChecked == false && (seg.Contains("Elevated") || seg.Contains("Slope")))
                                skip = true;
                            else if (delTypes[(int)p.Roads].isChecked == false && seg.Contains("Road"))
                                skip = true;
                            else if (delTypes[(int)p.Railroads].isChecked == false && seg.Contains("Train"))
                                skip = true;
                            else if (delTypes[(int)p.Highways].isChecked == false && seg.Contains("Highway"))
                                skip = true;
                            else if (delTypes[(int)p.Pipes].isChecked == false && seg.Contains("Pipe"))
                                skip = true;
                            else if (delTypes[(int)p.PowerLines].isChecked == false && seg.Contains("Power"))
                                skip = true;
                            else if (delTypes[(int)p.Ground].isChecked == false && (seg.Contains("Tunnel") == false && seg.Contains("Slope") == false && seg.Contains("Elevated") == false))
                                skip = true;

                          WriteLog("Will the segment named, " + segment.Info.name + ", be delected? That is " + skip + ".");

                            if (skip == false)
                            {
                                segmentsToDelete.Add(num5);
                            }
                        }
                        num5 = NetManager.instance.m_segments.m_buffer[(int)((UIntPtr)num5)].m_nextGridSegment;
                        if (++num6 >= 262144)
                        {
                            CODebugBase<LogChannel>.Error(LogChannel.Core, "Invalid list detected!\n" + Environment.StackTrace);
                            break;
                        }
                    }
                }
            }

            foreach (var segment in segmentsToDelete)
            {
                SimulationManager.instance.AddAction(this.ReleaseSegment(segment));
            }
            NetManager.instance.m_nodesUpdated = true;
        }

        private IEnumerator ReleaseSegment(ushort segment)
        {
            ToolBase.ToolErrors errors = ToolErrors.None;
            if (CheckSegment(segment, ref errors))
            {
                NetManager.instance.ReleaseSegment(segment, false);
            }
            yield return null;
        }

        protected void BulldozeBuildings()
        {
            List<ushort> buildingsToDelete = new List<ushort>();

            var minX = this.m_startPosition.x < this.m_mousePosition.x ? this.m_startPosition.x : this.m_mousePosition.x;
            var minZ = this.m_startPosition.z < this.m_mousePosition.z ? this.m_startPosition.z : this.m_mousePosition.z;
            var maxX = this.m_startPosition.x > this.m_mousePosition.x ? this.m_startPosition.x : this.m_mousePosition.x;
            var maxZ = this.m_startPosition.z > this.m_mousePosition.z ? this.m_startPosition.z : this.m_mousePosition.z;

            int gridMinX = Mathf.Max((int)((minX - 16f) / 64f + 135f), 0);
            int gridMinZ = Mathf.Max((int)((minZ - 16f) / 64f + 135f), 0);
            int gridMaxX = Mathf.Min((int)((maxX + 16f) / 64f + 135f), 269);
            int gridMaxZ = Mathf.Min((int)((maxZ + 16f) / 64f + 135f), 269);

            for (int i = gridMinZ; i <= gridMaxZ; i++)
            {
                for (int j = gridMinX; j <= gridMaxX; j++)
                {
                    ushort num5 = BuildingManager.instance.m_buildingGrid[i * 270 + j];
                    int num6 = 0;
                    while (num5 != 0u)
                    {
                        var building = BuildingManager.instance.m_buildings.m_buffer[(int)((UIntPtr)num5)];

                        Vector3 position = building.m_position;
                        float positionDiff = Mathf.Max(Mathf.Max(minX - 16f - position.x, minZ - 16f - position.z), Mathf.Max(position.x - maxX - 16f, position.z - maxZ - 16f));
                        if (positionDiff < 0f && building.m_parentBuilding <= 0)
                        {
                            buildingsToDelete.Add(num5);
                        }
                        num5 = BuildingManager.instance.m_buildings.m_buffer[(int)((UIntPtr)num5)].m_nextGridBuilding;
                        if (++num6 >= 262144)
                        {
                            CODebugBase<LogChannel>.Error(LogChannel.Core, "Invalid list detected!\n" + Environment.StackTrace);
                            break;
                        }
                    }
                }
            }

            foreach (ushort building in buildingsToDelete)
            {
                SimulationManager.instance.AddAction(this.ReleaseBuilding(building));
            }
        }

        private IEnumerator ReleaseBuilding(ushort building)
        {
            BuildingManager.instance.ReleaseBuilding(building);
            yield return null;
        }

        protected void BulldozeTrees()
        {
            List<uint> treesToDelete = new List<uint>();
            var minX = this.m_startPosition.x < this.m_mousePosition.x ? this.m_startPosition.x : this.m_mousePosition.x;
            var minZ = this.m_startPosition.z < this.m_mousePosition.z ? this.m_startPosition.z : this.m_mousePosition.z;
            var maxX = this.m_startPosition.x > this.m_mousePosition.x ? this.m_startPosition.x : this.m_mousePosition.x;
            var maxZ = this.m_startPosition.z > this.m_mousePosition.z ? this.m_startPosition.z : this.m_mousePosition.z;

            int num = Mathf.Max((int)((minX - 8f) / 32f + 270f), 0);
            int num2 = Mathf.Max((int)((minZ - 8f) / 32f + 270f), 0);
            int num3 = Mathf.Min((int)((maxX + 8f) / 32f + 270f), 539);
            int num4 = Mathf.Min((int)((maxZ + 8f) / 32f + 270f), 539);
            for (int i = num2; i <= num4; i++)
            {
                for (int j = num; j <= num3; j++)
                {
                    uint num5 = TreeManager.instance.m_treeGrid[i * 540 + j];
                    int num6 = 0;
                    while (num5 != 0u)
                    {
                        var tree = TreeManager.instance.m_trees.m_buffer[(int)((UIntPtr)num5)];
                        Vector3 position = tree.Position;
                        float num7 = Mathf.Max(Mathf.Max(minX - 8f - position.x, minZ - 8f - position.z), Mathf.Max(position.x - maxX - 8f, position.z - maxZ - 8f));
                        if (num7 < 0f)
                        {

                            treesToDelete.Add(num5);
                        }
                        num5 = TreeManager.instance.m_trees.m_buffer[(int)((UIntPtr)num5)].m_nextGridTree;
                        if (++num6 >= 262144)
                        {
                            CODebugBase<LogChannel>.Error(LogChannel.Core, "Invalid list detected!\n" + Environment.StackTrace);
                            break;
                        }
                    }
                }
            }
            foreach (uint tree in treesToDelete)
            {
                TreeManager.instance.ReleaseTree(tree);
            }
            TreeManager.instance.m_treesUpdated = true;
        }

        protected void BulldozeProps()
        {
            List<ushort> propsToDelete = new List<ushort>();
            var minX = this.m_startPosition.x < this.m_mousePosition.x ? this.m_startPosition.x : this.m_mousePosition.x;
            var minZ = this.m_startPosition.z < this.m_mousePosition.z ? this.m_startPosition.z : this.m_mousePosition.z;
            var maxX = this.m_startPosition.x > this.m_mousePosition.x ? this.m_startPosition.x : this.m_mousePosition.x;
            var maxZ = this.m_startPosition.z > this.m_mousePosition.z ? this.m_startPosition.z : this.m_mousePosition.z;

            int num = Mathf.Max((int)((minX - 16f) / 64f + 135f), 0);
            int num2 = Mathf.Max((int)((minZ - 16f) / 64f + 135f), 0);
            int num3 = Mathf.Min((int)((maxX + 16f) / 64f + 135f), 269);
            int num4 = Mathf.Min((int)((maxZ + 16f) / 64f + 135f), 269);
            for (int i = num2; i <= num4; i++)
            {
                for (int j = num; j <= num3; j++)
                {
                    ushort num5 = PropManager.instance.m_propGrid[i * 270 + j];
                    int num6 = 0;
                    while (num5 != 0u)
                    {
                        var prop = PropManager.instance.m_props.m_buffer[(int)((UIntPtr)num5)];
                        Vector3 position = prop.Position;
                        float num7 = Mathf.Max(Mathf.Max(minX - 16f - position.x, minZ - 16f - position.z), Mathf.Max(position.x - maxX - 16f, position.z - maxZ - 16f));

                        if (num7 < 0f)
                        {
                            propsToDelete.Add(num5);
                        }
                        num5 = PropManager.instance.m_props.m_buffer[(int)((UIntPtr)num5)].m_nextGridProp;
                        if (++num6 >= 262144)
                        {
                            CODebugBase<LogChannel>.Error(LogChannel.Core, "Invalid list detected!\n" + Environment.StackTrace);
                            break;
                        }
                    }
                }
            }
            foreach (ushort prop in propsToDelete)
            {
                PropManager.instance.ReleaseProp(prop);
            }
            PropManager.instance.m_propsUpdated = true;
        }

        private string GetMessage(ToolErrors errors)
        {
            string text = "";
            if (errors == ToolErrors.None) text += "";
            if (errors == ToolErrors.OutOfArea) text += "Out of city limits!";
            else if (errors == ToolErrors.AlreadyExists) text += "Already exists";
            else if (errors == ToolErrors.CannotBuildOnWater) text += "Cannot build on water";
            else if (errors == ToolErrors.InvalidShape) text += "Invalid Shape";
            else if (errors == ToolErrors.NotEnoughMoney) text += "Not enough money";
            else if (errors == ToolErrors.OutOfArea) text += "Out of Area";
            else if (errors == ToolErrors.CannotUpgrade) text += "Cannot upgrade to this type";
            else text += "Ondefined error: " + errors;

            return text;
        }

        protected bool ValidateSelectedArea(NetSegment segment)
        {
            var minX = this.m_startPosition.x < this.m_mousePosition.x ? this.m_startPosition.x : this.m_mousePosition.x;
            var minZ = this.m_startPosition.z < this.m_mousePosition.z ? this.m_startPosition.z : this.m_mousePosition.z;
            var maxX = this.m_startPosition.x > this.m_mousePosition.x ? this.m_startPosition.x : this.m_mousePosition.x;
            var maxZ = this.m_startPosition.z > this.m_mousePosition.z ? this.m_startPosition.z : this.m_mousePosition.z;

            Vector3 position = segment.m_middlePosition;
            float positionDiff = Mathf.Max(Mathf.Max(minX - 16f - position.x, minZ - 16f - position.z), Mathf.Max(position.x - maxX - 16f, position.z - maxZ - 16f));

            if (positionDiff < 0f)
            {
                return true;
            }
            return false;
        }

        void GetSegmentControlPoints(int segmentIndex, out NetTool.ControlPoint startPoint, out NetTool.ControlPoint middlePoint, out NetTool.ControlPoint endPoint)
        {
            NetManager net = Singleton<NetManager>.instance;
            startPoint = new NetTool.ControlPoint();
            middlePoint = new NetTool.ControlPoint();
            endPoint = new NetTool.ControlPoint();

            if (segmentIndex >= net.m_segments.m_buffer.Length)
                return;

            NetInfo prefab = net.m_segments.m_buffer[segmentIndex].Info;

            startPoint.m_node = net.m_segments.m_buffer[segmentIndex].m_startNode;
            startPoint.m_segment = 0;
            startPoint.m_position = net.m_nodes.m_buffer[startPoint.m_node].m_position;
            startPoint.m_direction = net.m_segments.m_buffer[segmentIndex].m_startDirection;
            startPoint.m_elevation = net.m_nodes.m_buffer[startPoint.m_node].m_elevation;
            startPoint.m_outside = (net.m_nodes.m_buffer[startPoint.m_node].m_flags & NetNode.Flags.Outside) != NetNode.Flags.None;

            endPoint.m_node = net.m_segments.m_buffer[segmentIndex].m_endNode;
            endPoint.m_segment = 0;
            endPoint.m_position = net.m_nodes.m_buffer[endPoint.m_node].m_position;
            endPoint.m_direction = -net.m_segments.m_buffer[segmentIndex].m_endDirection;
            endPoint.m_elevation = net.m_nodes.m_buffer[endPoint.m_node].m_elevation;
            endPoint.m_outside = (net.m_nodes.m_buffer[endPoint.m_node].m_flags & NetNode.Flags.Outside) != NetNode.Flags.None;
            
            middlePoint.m_node = 0;
            middlePoint.m_segment = (ushort)segmentIndex;
            middlePoint.m_position = startPoint.m_position + startPoint.m_direction * (prefab.GetMinNodeDistance() + 1f);
            middlePoint.m_direction = startPoint.m_direction;
            middlePoint.m_elevation = Mathf.Lerp(startPoint.m_elevation, endPoint.m_elevation, 0.5f);
            middlePoint.m_outside = false;
        }

        private void ApplyServices()
        {

        }

        protected void ApplyUpdates()
        {
            //test our absolutes selected area for tiny selections and ignore them
            string xy = Math.Abs(m_startPosition.x - m_mousePosition.x) + " : " + Math.Abs(m_startPosition.y - m_mousePosition.y);
            if (Math.Abs(m_startPosition.x - m_mousePosition.x) < 20)
                {
                if (Math.Abs(m_startPosition.y - m_mousePosition.y) < 20)
                {
                    return;
                }
            } 
            if (fromSelected == string.Empty)
                return;
            if (toSelected == string.Empty)
                return;
            try
            {
                int totalCost = 0;
                ToolBase.ToolErrors errors;
                ConvertObjects(toSelected, fromSelected, false, out totalCost, out errors);
            }
            catch (Exception)
            {
                throw;
            }
        }

        protected void ApplyBulldoze()
        {
            //if there are no deletes exit
            if (cbDelete.isVisible == false || cbDelete.isChecked == false) { return; }

            if (delTypes[(int)p.Trees].isChecked)
                BulldozeTrees();
            try
            {
                if (delTypes[(int)p.Roads].isChecked || delTypes[(int)p.Railroads].isChecked || delTypes[(int)p.Highways].isChecked || delTypes[(int)p.Pipes].isChecked || delTypes[(int)p.PowerLines].isChecked)
                {
                    BulldozeRoads();
                }
            }
            catch (Exception)
            {
                throw;
            }

            if (delTypes[(int)p.Buildings].isChecked)
                BulldozeBuildings();
            if (delTypes[(int)p.Props].isChecked)
                BulldozeProps();
        }

        #endregion

        #region "Logging"

        public static void WriteLog(string data)
        {
            WriteLog(data, false);
        }

        public static void WriteLog(string data, bool delete)
        {
            string filename = "AnotherRoadUpdater" + DateTime.Now.ToString("yyyyMMdd") + ".txt";
            if (delete)
            {
                File.Delete(filename);
            }

            // Write the string to a file.
            System.IO.StreamWriter file = File.AppendText(filename);
            file.WriteLine(data);
            file.Close();
        }
    }

    #endregion

    #endregion
}