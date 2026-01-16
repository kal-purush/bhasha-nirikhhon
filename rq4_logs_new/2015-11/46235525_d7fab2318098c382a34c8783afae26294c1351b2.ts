namespace component {

  export function SomeComponent() {
    return {
      scope: {},
      controller: ComponentCtrl,
      controllerAs: "componentctrl",
      template: `
        <div>{{componentctrl.msg}}</div>
      `
    };
  }

  class ComponentCtrl {
    msg = "hello from component";
  }
}