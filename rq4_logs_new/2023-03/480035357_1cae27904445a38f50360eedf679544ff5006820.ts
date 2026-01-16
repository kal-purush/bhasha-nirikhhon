import { shallowMount } from "@vue/test-utils";
import {
  baseClassCase,
  callWhenClickCase,
  emitClickEventCase,
  tagCase,
} from "@/utils/test-case-factories";
import DBackdrop from "@/components/atoms/d-backdrop";
import config from "./config";

describe("DButton", () => {
  const wrapper = shallowMount(DBackdrop);
  baseClassCase(wrapper, config.className);

  it("Should render props.opacity to the component style as '--opacity: props.opacity'", async () => {
    const opacity = 0.33;
    await wrapper.setProps({
      opacity,
    });
    expect(wrapper.attributes("style")).toContain(`--opacity: ${opacity}`);
  });

  it("Should render props.zIndex to the component style as '--z-index: props.zIndex'", async () => {
    const zIndex = 0.33;
    await wrapper.setProps({
      zIndex,
    });
    expect(wrapper.attributes("style")).toContain(`--z-index: ${zIndex}`);
  });

  it("Should render props.colorScheme to the component style as '--background-color: `var(--color-${this.colorScheme}-background)`'", async () => {
    const zIndex = 0.33;
    await wrapper.setProps({
      zIndex,
    });
    expect(wrapper.attributes("style")).toContain(`--z-index: ${zIndex}`);
  });

  tagCase(wrapper);

  emitClickEventCase(wrapper);

  callWhenClickCase(wrapper);
});