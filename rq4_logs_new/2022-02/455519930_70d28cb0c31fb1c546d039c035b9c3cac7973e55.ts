import { defineComponent } from "vue";
import { Style } from "@sil/tools";
import { useColorMode } from "../../composables/useColorMode";

export default defineComponent({
  setup() {
    const { colorMode, oppositeMode, switchColorMode } = useColorMode();
    const style = new Style("color-mode");
    return {
      style,
      colorMode,
      oppositeMode,
      switchColorMode,
    };
  },
});