import { useForm, SubmitHandler } from "react-hook-form";
import { resolver } from "../utils/validation";
import { Task } from "../types/Task";
import { FormData } from "../types/FormData";
import { getPokemonData } from "../utils/api/getPokemonData";

export const useAdd = (
  setTasks: React.Dispatch<React.SetStateAction<Task[]>>
) => {
  const {
    register,
    handleSubmit,
    formState: { errors },
    reset,
  } = useForm<FormData>({
    resolver: resolver,
  });

  const onSubmit: SubmitHandler<FormData> = async (task) => {
    const pokemonData = await getPokemonData(Number(task.assignedPokemon));
    const newTask: Task = {
      id: Date.now(),
      title: task.title,
      assignedPokemon: pokemonData.name,
      assignedPokemonImage: pokemonData.image,
    };

    setTasks((prevTasks) => [...prevTasks, newTask]);

    reset();
  };

  return {
    register,
    handleSubmit,
    errors,
    onSubmit,
  };
};