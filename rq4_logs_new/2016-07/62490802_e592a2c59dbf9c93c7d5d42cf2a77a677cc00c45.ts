import {ProjectManager} from '../../src/shared/project-manager';

describe('ProjectManager removeProject', () => {
  let sut: ProjectManager;
  let state;

  beforeEach(() => {
    state = {
      projects: [],
      _save: jasmine.createSpy('_save')
    };
    sut = new ProjectManager(null, state);
  });

  it('removes project from state', () => {
    let deleteProject = {
      name: 'foo'
    };
    state.projects.push({
      name: 'My project'
    });
    state.projects.push(deleteProject);
    state.projects.push({
      name: 'bar'
    });

    sut.removeProject(deleteProject);
    expect(state.projects.length).toBe(2);
    expect(state.projects.indexOf(deleteProject)).toBe(-1);
  });

  it('persists changes to session', () => {
    let deleteProject = {
      name: 'foo'
    };
    state.projects.push({
      name: 'My project'
    });
    state.projects.push(deleteProject);
    state.projects.push({
      name: 'bar'
    });

    sut.removeProject(deleteProject);
    expect(state._save).toHaveBeenCalled();
  });
});


describe('ProjectManager hasProjects', () => {
  let sut: ProjectManager;
  let state;

  beforeEach(() => {
    state = {
      projects: []
    };
    sut = new ProjectManager(null, state);
  });

  it('returns whether the projectmanager has any projects registered', () => {
    expect(sut.hasProjects()).toBe(false);

    state.projects.push({
      name: 'Foo'
    });

    expect(sut.hasProjects()).toBe(true);
  });
});


describe('ProjectManager addProjectByWizardState', () => {
  let sut: ProjectManager;
  let state;

  beforeEach(() => {
    state = {
      projects: []
    };
    sut = new ProjectManager(null, state);
  });

  it('calls addProject with project definition based on wizard state', () => {
    let addProjectSpy = spyOn(sut, 'addProject');

    sut.addProjectByWizardState({
      installNPM: true,
      path: 'c:/some/dir',
      name: 'foo'
    });

    expect(addProjectSpy).toHaveBeenCalledWith({
      installNPM: true,
      path: 'c:/some/dir',
      name: 'foo'
    });
  });
});


describe('ProjectManager addProjectByPath', () => {
  let sut: ProjectManager;
  let state;

  beforeEach(() => {
    state = {
      projects: []
    };
    sut = new ProjectManager(null, state);
  });

  it('calls addProject with project path', () => {
    let addProjectSpy = spyOn(sut, 'addProject');

    sut.addProjectByPath('c:/some/dir');

    expect(addProjectSpy).toHaveBeenCalledWith({
      path: 'c:/some/dir'
    });
  });
});