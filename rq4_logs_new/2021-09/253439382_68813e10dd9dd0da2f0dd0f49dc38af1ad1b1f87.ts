import { shallowMount } from "@vue/test-utils";
import Axioms from "@/components/concept/definition/Axioms.vue";
import Button from "primevue/button";
import StyleClass from "primevue/styleclass";

describe("Axioms.vue ___ data", () => {
  let wrapper: any;

  beforeEach(() => {
    jest.resetAllMocks();

    wrapper = shallowMount(Axioms, {
      global: {
        components: { Button },
        directives: { styleclass: StyleClass }
      },
      props: {
        label: "Stated axioms",
        size: "100%",
        data: {
          axiomString:
            "Equivalent class\n    Intersection of\n        Disease (disorder)\n        Having type Restriction on property role group\n            Some values from\n                Intersection of\n                    Having type Restriction on property Associated morphology (attribute)\n                        Some values from\n                            Lateral abnormal curvature (morphologic abnormality)\n                    Having type Restriction on property Finding site (attribute)\n                        Some values from\n                            Musculoskeletal structure of spine (body structure)",
          count: 1
        }
      }
    });
  });

  it("can set variables on mounted", () => {
    expect(wrapper.vm.buttonExpanded).toBe(false);
    expect(wrapper.vm.axiomString).toBe(
      "Equivalent class\n    Intersection of\n        Disease (disorder)\n        Having type Restriction on property role group\n            Some values from\n                Intersection of\n                    Having type Restriction on property Associated morphology (attribute)\n                        Some values from\n                            Lateral abnormal curvature (morphologic abnormality)\n                    Having type Restriction on property Finding site (attribute)\n                        Some values from\n                            Musculoskeletal structure of spine (body structure)"
    );
    expect(wrapper.vm.count).toBe(1);
  });

  it("can set button expanded ___ true", () => {
    expect(wrapper.vm.buttonExpanded).toBe(false);
    wrapper.vm.setButtonExpanded();
    expect(wrapper.vm.buttonExpanded).toBe(true);
  });

  it("can set button expanded ___ false", () => {
    wrapper.vm.buttonExpanded = true;
    expect(wrapper.vm.buttonExpanded).toBe(true);
    wrapper.vm.setButtonExpanded();
    expect(wrapper.vm.buttonExpanded).toBe(false);
  });
});

describe("Axioms.vue ___ missing keys data", () => {
  let wrapper: any;

  beforeEach(() => {
    jest.resetAllMocks();

    wrapper = shallowMount(Axioms, {
      global: {
        components: { Button },
        directives: { styleclass: StyleClass }
      },
      props: {
        label: "Stated axioms",
        size: "100%",
        data: {
          axiom:
            "Equivalent class\n    Intersection of\n        Disease (disorder)\n        Having type Restriction on property role group\n            Some values from\n                Intersection of\n                    Having type Restriction on property Associated morphology (attribute)\n                        Some values from\n                            Lateral abnormal curvature (morphologic abnormality)\n                    Having type Restriction on property Finding site (attribute)\n                        Some values from\n                            Musculoskeletal structure of spine (body structure)",
          counter: 1
        }
      }
    });
  });

  it("can set variables on mounted", () => {
    expect(wrapper.vm.buttonExpanded).toBe(false);
    expect(wrapper.vm.axiomString).toBe("None");
    expect(wrapper.vm.count).toBe(0);
  });
});