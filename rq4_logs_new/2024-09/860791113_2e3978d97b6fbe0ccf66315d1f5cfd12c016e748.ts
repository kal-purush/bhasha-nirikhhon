import { useModelStore } from "../store/model.store";

export const useModelLoader = () => {
  const {
    loadedFile,
    setLoadedFile,
    isLoading,
    setIsLoading,
    error,
    setError,
  } = useModelStore();

  const loadExistingModel = async (modelName: string) => {
    setIsLoading(true);
    setError(null);
    try {
      console.log(`Loading model: ${modelName}`); // Log the model name being loaded
      const response = await fetch(`/${modelName}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const blob = await response.blob();
      const file = new File([blob], modelName, {
        type: "application/octet-stream",
      });
      setLoadedFile(file);
      console.log(`Successfully loaded file: ${file.name}`); // Log the successfully loaded file name
    } catch (err) {
      console.error(`Error loading model: ${modelName}`, err); // Log any errors
      setError(err instanceof Error ? err.message : "An error occurred");
    } finally {
      setIsLoading(false);
    }
  };

  const loadUploadedModel = (file: File) => {
    setLoadedFile(file);
  };

  return {
    isLoading,
    error,
    loadedFile,
    loadExistingModel,
    loadUploadedModel,
    setLoadedFile,
  };
};