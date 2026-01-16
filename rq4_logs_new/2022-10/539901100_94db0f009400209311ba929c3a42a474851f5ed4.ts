import fs from "fs";
import path from "path";
import matter from "gray-matter";
import { remark } from "remark";
import html from "remark-html";

const projectsDir = path.join(process.cwd(), "projects");

export type ProjectData = {
  image: string,
	title: string,
  date?: number,
	position?: boolean,
  text: string,
}


export const getAllProjects = () => {
  const projectNames = fs.readdirSync(projectsDir);

  return projectNames.map((projectName) => {
    return {
      params : {
        project: projectName.replace(/\.md$/, ""),
      }
    }
  });
}

export const getProjectData = async (name: String) : Promise<ProjectData> => {
  const fileContents = fs.readFileSync(path.join(projectsDir, `${name}.md`), "utf8");
  // Use matter to split the metadata from the content in the .md file
  const matterConversed = matter(fileContents);

  const htmlContent = await remark()
    .use(html)
    .process(matterConversed.content);


  return <ProjectData> {
    ...matterConversed.data,
    text: htmlContent.toString(),
  }
};

export const getAllProjectsData = async () : Promise<Array<ProjectData>> => {
  const projectNames = fs.readdirSync(projectsDir);
  let allProjectsData = projectNames.map(async (projectName) : Promise<ProjectData> => { 
    return <ProjectData> (await getProjectData(projectName.replace(/\.md$/, "")));
  });
  
  return await Promise.all(allProjectsData);
}

export const getAllProjectDataSorted = async () : Promise<Array<ProjectData>> => {
  return (await getAllProjectsData()).sort((a, b) => {
    if((a.date || (new Date()).getTime()) < (b.date || (new Date()).getTime())) return 1;
    else return -1;
  });
}

