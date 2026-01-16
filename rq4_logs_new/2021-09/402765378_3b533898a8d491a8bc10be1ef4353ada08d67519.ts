import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import data from "../data.json";
import { Projects, Project } from "../types/Interface";

const initialState: Projects = {
  projects: [],
};

const initialProject: Project = {
  projectName: "",
  estimatedBy: "",
  estimationDate: [],
  verifiedBy: "",
  verificationDate: "",
  estStart: "",
  estEns: "",
  teamSize: 0,
  timeBudget: 0,
  effort: 0,
  sections: [],
  rawDevelopmentEffortSum: {
    name: "",
    main: {},
    parts: [],
  },

  summary: [],
  assumptions: [],
};

const shortDate = () => {
  var dateObj = new Date();
  var month = ("0" + (dateObj.getMonth() + 1)).slice(-2);
  var date = ("0" + dateObj.getDate()).slice(-2);
  var year = dateObj.getFullYear();
  var shortDate = date + "." + month + "." + year;
  return shortDate;
};

// const generateProject = (name: string): Project => ({
//     ...initialProject,
//     projectName: name
// })

const projectSlice = createSlice({
  name: "project",
  initialState,
  reducers: {
    initialProjects: (state) => {
      const projects: any[] = [...data.projects];
      state.projects = projects;
    },
    clearProjects: (state) => {
      state.projects = [];
    },
    addProject: (state, action: PayloadAction<Project>) => {
      //   var dateObj = new Date();
      //   var month = ("0" + (dateObj.getMonth() + 1)).slice(-2);
      //   var date = ("0" + dateObj.getDate()).slice(-2);
      //   var year = dateObj.getFullYear();
      //   var shortDate = date + "." + month + "." + year;
      const newProject = Object.assign({}, initialProject, {
        projectName: action.payload.projectName,
        estimatedBy: action.payload.estimatedBy,
        estimationDate: shortDate(),
      });
      state.projects.push(newProject);
    },
  },
});

// Action creators are generated for each case reducer function
export const { initialProjects, clearProjects, addProject } =
  projectSlice.actions;

const projectReducer = projectSlice.reducer;

export default projectReducer;
//add task
//add sub
//del task, sub, proj
//add proj -> done