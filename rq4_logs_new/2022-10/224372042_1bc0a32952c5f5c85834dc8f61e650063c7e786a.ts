import type { GatsbyNode } from "gatsby";
import Path from "path";

export const onCreateWebpackConfig: GatsbyNode["onCreateWebpackConfig"] = ({ actions }) => {
  actions.setWebpackConfig({
    resolve: {
      modules: [Path.resolve(__dirname, "src"), "node_modules"],
    },
  });
};