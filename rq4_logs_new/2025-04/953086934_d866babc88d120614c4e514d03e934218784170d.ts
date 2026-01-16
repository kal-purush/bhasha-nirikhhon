import { useMutation, useQueryClient } from "@tanstack/react-query";
import { addDoc, deleteDoc, doc, setDoc } from "firebase/firestore";
import { useSnackbar } from "notistack";

import clients from "@/lib/collections/firebase/clients";
import type { Client, ClientData } from "@/types/firebase";

const useClients = () => {
  /** Values */

  const queryClient = useQueryClient();
  const { enqueueSnackbar } = useSnackbar();

  const create = useMutation({
    mutationKey: [clients.id, "create"],
    mutationFn: (data: ClientData) => addDoc(clients, data),
    onSuccess: (_, data) => {
      queryClient.invalidateQueries({ queryKey: [clients.id] });
      enqueueSnackbar(`'${data.first_name} ${data.last_name}' client created`, {
        variant: "success",
      });
    },
    onError: () =>
      enqueueSnackbar("Error creating client", {
        variant: "error",
      }),
  });

  const update = useMutation({
    mutationKey: [clients.id, "update"],
    mutationFn: async ({ id, ...data }: Client) => {
      const docRef = doc(clients, id);
      await setDoc(docRef, { ...data });
    },
    onSuccess: (_, data) => {
      queryClient.invalidateQueries({ queryKey: [clients.id] });
      enqueueSnackbar(`'${data.first_name} ${data.last_name}' client updated`, {
        variant: "success",
      });
    },
    onError: () =>
      enqueueSnackbar("Error updating client", {
        variant: "error",
      }),
  });

  const archive = useMutation({
    mutationKey: [clients.id, "archive"],
    mutationFn: async (data: Client["id"]) => {
      const docRef = doc(clients, data);
      await deleteDoc(docRef);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [clients.id] });
      enqueueSnackbar("Client archived", {
        variant: "success",
      });
    },
    onError: () =>
      enqueueSnackbar("Error archiving client", {
        variant: "error",
      }),
  });

  return { create, update, archive };
};

export default useClients;