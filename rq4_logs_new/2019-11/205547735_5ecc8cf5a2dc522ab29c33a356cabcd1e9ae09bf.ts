import {Theme, createStyles} from "@material-ui/core"
import {RootContainerStyles} from "@renderer/common/styles/RootStyles"

export const styles = (theme: Theme) =>
  createStyles({
    root: {
      ...RootContainerStyles(theme).root,
    },
    title: {
      ...RootContainerStyles(theme).title,
    },
  })