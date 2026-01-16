﻿using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Windows.Input;
using System.Windows.Forms;
using System.Drawing;
using Microsoft.VisualStudio.TestTools.UITesting;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.VisualStudio.TestTools.UITest.Extension;
using Keyboard = Microsoft.VisualStudio.TestTools.UITesting.Keyboard;
using ezLogUITest;
using Microsoft.VisualStudio.TestTools.UITesting.WinControls;
using System.Timers;


namespace RMSCodedUITestProject
{
    /// <summary>
    /// DeleteTest 的摘要描述
    /// </summary>
    [CodedUITest]
    public class TestGUITestDelete
    {
        private string FILE_PATH = "../../../RMS_Project/bin/debug/RMS_Project.exe";
        private string UI_TESTING_EXAMPLE_TITLE = "RMS_Project";
        private UITestControl _root;

        [TestInitialize()]
        public void Initialize()
        {
            _root = Robot.Initialize(FILE_PATH, UI_TESTING_EXAMPLE_TITLE);
        }

        [TestCleanup()]
        public void Cleanup()
        {
            Robot.CleanUp();
        }

        [TestMethod]
        [DeploymentItem("RMS_Project.exe")]
        public void CheckDeleteTest()
        {
            Robot.SetOtherFormEdit("loginForm", "emailLabel", "j00064qaz123@gmail.com");

            Robot.SetOtherFormUseSystemPasswordChar("loginForm", "passwordLabel", "a123456");

            Robot.AssertOtherFormEdit("loginForm", "emailLabel", "j00064qaz123@gmail.com");
            Robot.ClickOtherFormButton("loginForm", "signInButton");
            Robot.AssertWindowExist("projectListForm", true);

            Robot.ClickDataGridView("projectListForm", "projectDataGridView", 0, 0, 550, 3);
            Robot.AssertWindowExist("ProjectMainForm", true);

            Robot.ClickOtherFormButton("ProjectMainForm", "testButton");

            Robot.AssertWindowExist("testListForm", true);

            Robot.ClickOtherFormButton("UserInterfaceForm", "NewProjectButton");

            Robot.SetOtherFormEdit("testEditorForm", "testName", "login test Error");
            Robot.SetOtherFormEdit("testEditorForm", "testInputData", "test2@test/a123456");
            Robot.SetOtherFormEdit("testEditorForm", "testResult", "fail");
            Robot.ClickOtherFormComboBox("testEditorForm", "ownerCombobox", "ZZ");
            Robot.SetOtherFormEdit("testEditorForm", "testDescription", "Test login account and password is false");
            Robot.CheckTheCheckedListBox("testEditorForm", "checkedListBox", 0, true);
            Robot.ClickOtherFormButton("testEditorForm", "testConfirm");

            Robot.AssertWindowExist("Success", true);
            Robot.ClickOtherFormButton("Success", "確定");

            Robot.ClickDataGridView("testListForm", "testListDataGridView", 0, 2, 5, 3);

            Robot.AssertWindowExist("Success", true);
            Robot.ClickOtherFormButton("Success", "確定");

            Robot.AssertDataGridViewNumericUpDownCellValue("testListForm", "testListDataGridView", 0, 0, "login test Error");
        }

        [TestMethod]
        [DeploymentItem("RMS_Project.exe")]
        public void CheckRequirmentDetail()
        {
            Robot.SetOtherFormEdit("loginForm", "emailLabel", "j00064qaz123@gmail.com");

            Robot.SetOtherFormUseSystemPasswordChar("loginForm", "passwordLabel", "a123456");

            Robot.AssertOtherFormEdit("loginForm", "emailLabel", "j00064qaz123@gmail.com");
            Robot.ClickOtherFormButton("loginForm", "signInButton");
            Robot.AssertWindowExist("projectListForm", true);

            Robot.ClickDataGridView("projectListForm", "projectDataGridView", 0, 0, 550, 3);
            Robot.AssertWindowExist("ProjectMainForm", true);


            Robot.ClickOtherFormButton("projectMainForm", "requirementButton");


            Robot.ClickDataGridView("requirementListForm", "requirementListDataGridView", 0, 0, 300, 3);

            Robot.AssertWindowExist("requirementDetailForm", true);

            //associatedTestListDataGridView

            Robot.AssertDataGridViewNumericUpDownCellValue("requirementDetailForm", "associatedTestListDataGridView", 0, 0, "login test Error");

        }
    }
}