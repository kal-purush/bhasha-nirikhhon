import Vue from 'vue';

/**
 * Loads components and register them as global component.
 * @see https://vuejs.org/v2/guide/components-registration.html#Automatic-Global-Registration-of-Base-Components
 */
function loadGlobalComponents() {
  const requireComponent = require.context('.', true, /[A-Z]\w+\.(vue|js)$/);

  requireComponent.keys().forEach((fileName: string) => {
    const componentConfig = requireComponent(fileName);

    // Get PascalCase name of component
    const componentName = (fileName.split('/').pop() ?? '').replace(/\.\w+$/, '');

    // Register component globally
    Vue.component(componentName, componentConfig.default || componentConfig);
  });
}

loadGlobalComponents();