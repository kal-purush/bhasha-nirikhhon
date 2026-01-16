import Information from '../../../src/components/information/Information';

describe('Information.ts Test', () => {
  it('getUserName', () => {
    const component: Information = new Information();
    expect(component.getUserName()).toBe('userName');
  });

  it('getUserId', () => {
    const component: Information = new Information();
    expect(component.getUserId()).toBe('0f3c162b-41cb-44f3-af3b-8f66054eb450');
  });

  it('getNotificationButtonActive', () => {
    const component: Information = new Information();
    expect(component.getNotificationButtonActive()).toBe(false);
    component.activeNotificationButton();
    expect(component.getNotificationButtonActive()).toBe(true);
    component.inactiveNotificationButton();
    expect(component.getNotificationButtonActive()).toBe(false);
  });

  it('getMenuButtonActive', () => {
    const component: Information = new Information();
    expect(component.getMenuButtonActive()).toBe(false);
    component.activeMenuButton();
    expect(component.getMenuButtonActive()).toBe(true);
    component.inactiveMenuButton();
    expect(component.getMenuButtonActive()).toBe(false);
  });
});