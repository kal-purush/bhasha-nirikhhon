import Unocss from "unocss/vite";
import { presetUno, presetAttributify, presetIcons } from "unocss";

const colors = ["black", "gray", "red"];
const safelist = [...colors.map((v) => `bg-${v}-500`), ...["search", "edit", "check"].map((v) => `i-ic-baseline-${v}`)];
export default () =>
  Unocss({
    safelist,
    presets: [presetUno(), presetAttributify(), presetIcons()],
  });