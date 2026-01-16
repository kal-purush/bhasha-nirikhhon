import { projects } from '../directory';

describe('Projects Directory', () => {
  test('projects array should not be empty', () => {
    expect(projects.length).toBeGreaterThan(0);
  });
  
  test('each project should have required fields', () => {
    projects.forEach(project => {
      expect(project).toHaveProperty('type');
      expect(project).toHaveProperty('name');
      expect(project).toHaveProperty('oneLiner');
      expect(project).toHaveProperty('blurb');
      expect(project).toHaveProperty('logo');
      expect(project).toHaveProperty('banner');
      expect(project).toHaveProperty('tags');
    });
  });
  
  test('project types should be valid', () => {
    const validTypes = ['project', 'infrastructure', 'tooling'];
    projects.forEach(project => {
      expect(validTypes).toContain(project.type);
    });
  });
  
  test('tags should be an array of strings', () => {
    projects.forEach(project => {
      expect(Array.isArray(project.tags)).toBe(true);
      project.tags.forEach(tag => {
        expect(typeof tag).toBe('string');
      });
    });
  });
});