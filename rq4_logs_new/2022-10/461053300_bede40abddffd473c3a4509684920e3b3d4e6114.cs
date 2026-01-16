using Grasshopper.Kernel;
using Rhino.Geometry;
using Rhino.Geometry.Collections;
using System;
using System.Linq;
using System.Collections.Generic;
using Rhino.Geometry.Intersect;
using Newtonsoft.Json;
using Model;
using Utils;

// In order to load the result of this wizard, you will also need to
// add the output bin/ folder of this project to the list of loaded
// folder in Grasshopper.
// You can use the _GrasshopperDeveloperSettings Rhino command for that.

namespace THERBgh
{
    public class UpdateSetting : GH_Component
    {
        /// <summary>
        /// Each implementation of GH_Component must provide a public 
        /// constructor without any arguments.
        /// Category represents the Tab in which the component will appear, 
        /// Subcategory the panel. If you use non-existing tab or panel names, 
        /// new tabs/panels will automatically be created.
        /// </summary>
        public UpdateSetting()
          : base("updateSetting", "updateSetting",
              "update setting",
              "THERB-GH", "Modelling")
        {
        }

        /// <summary>
        /// Registers all the input parameters for this component.
        /// </summary>
        protected override void RegisterInputParams(GH_Component.GH_InputParamManager pManager)
        {
            pManager.AddVectorParameter("north_direction", "north_direction", "north direction", GH_ParamAccess.item);
            pManager.AddIntegerParameter("start_month", "start_month", "start month for simulation", GH_ParamAccess.item);
            pManager.AddIntegerParameter("end_month", "end_month", "end month for simulation", GH_ParamAccess.item);
            pManager.AddNumberParameter("ventilation rate[ACH]", "ventilation rate[ACH]", "ventilation rate[ACH]", GH_ParamAccess.item);
            pManager[0].Optional = true;
            pManager[1].Optional = true;
            pManager[2].Optional = true;
            pManager[3].Optional = true;
        }

        /// <summary>
        /// Registers all the output parameters for this component.
        /// </summary>
        protected override void RegisterOutputParams(GH_Component.GH_OutputParamManager pManager)
        {
            pManager.AddGenericParameter("Setting", "Setting", "Setting class", GH_ParamAccess.item);
            pManager.AddTextParameter("t_dat", "t_dat", "t.dat file", GH_ParamAccess.item);
        }

        /// <summary>
        /// This is the method that actually does the work.
        /// </summary>
        /// <param name="DA">The DA object can be used to retrieve data from input parameters and 
        /// to store data in output parameters.</param>
        protected override void SolveInstance(IGH_DataAccess DA)
        {
            int startMonth = 1;
            int endMonth = 12;
            double ventilationRate = 0.5;
            Vector3d northDirection = new Vector3d(0,0,0);

            DA.GetData("north_direction", ref northDirection);
            DA.GetData("start_month", ref startMonth);
            DA.GetData("end_month", ref endMonth);
            DA.GetData("ventilation rate[ACH]", ref ventilationRate);

            Setting setting = new Setting(northDirection, startMonth, endMonth, ventilationRate);

            DA.SetData("Setting",setting);
            DA.SetData("t_dat", CreateDatData.CreateTDat(startMonth, endMonth, northDirection));
        }

        /// <summary>
        /// Provides an Icon for every component that will be visible in the User Interface.
        /// Icons need to be 24x24 pixels.
        /// </summary>
        protected override System.Drawing.Bitmap Icon
        {
            get
            {
                // You can add image files to your project resources and access them like this:
                //return Resources.IconForThisComponent;
                return null;
            }
        }

        /// <summary>
        /// Each component must have a unique Guid to identify it. 
        /// It is vital this Guid doesn't change otherwise old ghx files 
        /// that use the old ID will partially fail during loading.
        /// </summary>
        public override Guid ComponentGuid
        {
            get { return new Guid("9df483e2-a9ed-45fc-9897-b0dc8bb8379e"); }
        }
    }
}