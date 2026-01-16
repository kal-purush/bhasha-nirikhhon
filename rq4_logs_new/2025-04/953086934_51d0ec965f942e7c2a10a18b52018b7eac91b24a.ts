import { useMutation } from "@tanstack/react-query";
import { useSnackbar } from "notistack";

import { updateProfile, type User } from "firebase/auth";

const useProfile = () => {
  /** Values */

  const { enqueueSnackbar } = useSnackbar();

  const update = useMutation({
    mutationKey: ["profile", "update"],
    mutationFn: (data: {
      user: User;
      data: Partial<Pick<User, "displayName" | "photoURL">>;
    }) => updateProfile(data.user, data.data),
    onSuccess: (_, data) =>
      enqueueSnackbar(
        `'${data.data.displayName ?? data.user.displayName ?? "User"}' updated`,
        { variant: "success" }
      ),
    onError: () =>
      enqueueSnackbar("Error updating profile", {
        variant: "error",
      }),
  });

  return { update };
};

export default useProfile;