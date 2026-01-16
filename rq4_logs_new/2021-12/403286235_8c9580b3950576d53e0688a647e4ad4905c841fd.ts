import { useCallback } from "react"
import { atom, useRecoilState } from "recoil"

interface SuccessSnackBar {
  open: boolean
  message: string
}

const successSnackBarState = atom<SuccessSnackBar>({
  key: "success-snackbar-state",
  default: { open: false, message: "" },
})

export function useSuccessSnackbar() {
  const [snackbar, setSnackBar] = useRecoilState(successSnackBarState)

  const openSuccessSnackbar = useCallback(
    (message: string) => {
      setSnackBar({ open: true, message: message })
    },
    [setSnackBar],
  )

  const closeSuccessSnackbar = useCallback(() => {
    setSnackBar({ open: false, message: "" })
  }, [setSnackBar])

  return {
    ...snackbar,
    openSuccessSnackbar: openSuccessSnackbar,
    closeSuccessSnackbar: closeSuccessSnackbar,
  }
}