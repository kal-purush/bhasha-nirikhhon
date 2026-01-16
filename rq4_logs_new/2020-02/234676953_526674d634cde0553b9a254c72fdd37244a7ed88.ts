import uuid from 'uuid';
import ProjectType from './project.type';
export class Project extends ProjectType {
  search: string;
  constructor(project: ProjectType) {
    super();
    Object.assign(this, project);
    this.search = this.generateSearch();
  }

  // this should be set as a global secondary index
  generateSearch(): string {
    const searchProperties: { [s: string]: string } = {
      userName: this.user.userName,
      projectType: this.projectType,
      visibility: this.visibility.toString(),
      startDate: this.startDate.toISOString(),
      endDate: this.endDate.toISOString(),
      createdAt: this.createdAt.toISOString(),
      updatedAt: this.updatedAt.toISOString(),
      tags: this.tags.join('__'),
      event: this.event || 'null',
      title: this.title
    };
    return Object.keys(searchProperties).reduce(
      (str: string, key: string) => `${str}${key}::${searchProperties[key]}`,
      ''
    );
  }
}

export const SeedProject = (): Project => {
  return new Project({
    id: uuid(),
    likedBy: [],
    followCount: 0,
    description: uuid(),
    coverImages: [uuid(), uuid()],
    collaborators: [
      {
        userName: uuid(),
        userIcon: uuid(),
        userId: uuid()
      }
    ],
    location: 'here',
    user: {
      userName: uuid(),
      userIcon: uuid(),
      userId: uuid()
    },
    projectType: 'testing',
    visibility: 'public',
    startDate: new Date(),
    endDate: new Date(),
    createdAt: new Date(),
    updatedAt: new Date(),
    tags: [uuid(), uuid()],
    // event: 'event',
    title: uuid()
  });
};