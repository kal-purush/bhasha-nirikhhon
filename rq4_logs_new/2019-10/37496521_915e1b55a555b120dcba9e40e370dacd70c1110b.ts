import { IntSliderView } from "@jupyter-widgets/controls";
import { WidgetManager } from "../../src/manager/manager";

describe("WidgetManager", () => {
  describe("loadClass", () => {
    it("returns a class if it exists", () => {
      const manager = new WidgetManager(document.createElement("div"));
      const view = manager.loadClass(
        "IntSliderView",
        "@jupyter-widgets/controls",
        "1.5.0"
      );
      expect(view).not.toBe(null);
    });
  });
  describe("create_view", () => {
    it("returns a widget mounted on the provided element", async () => {
      const manager = new WidgetManager(document.createElement("div"));
      const model = {
        _dom_classes: [],
        _model_module: "@jupyter-widgets/controls",
        _model_module_version: "1.5.0",
        _model_name: "IntSliderModel",
        _view_count: null,
        _view_module: "@jupyter-widgets/controls",
        _view_module_version: "1.5.0",
        _view_name: "IntSliderView",
        continuous_update: false,
        description: "Test:",
        description_tooltip: null,
        disabled: false,
        layout: "IPY_MODEL_db277618a5c443009a6aa5b07f6b1812",
        max: 10,
        min: 0,
        orientation: "horizontal",
        readout: true,
        readout_format: "d",
        step: 1,
        style: "IPY_MODEL_2e810dab21354e9381f53582e51e9a79",
        value: 7
      };
      const widget = await manager.create_view(model, {
        el: document.createElement("div"),
        model_id: "test_model_id"
      });
      expect(widget).not.toBeNull();
      expect(widget instanceof IntSliderView).toBe(true);
    });
  });
});