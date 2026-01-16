import { useLocalStore } from 'mobx-react-lite';

const useDataPromise = () => {
  const dataStore = useLocalStore(() => ({
    data: null,
    error: null,
    get loading() {
      return !!this.promise;
    },
    promise: null as Promise<any> | null,
    setPromise(promise: Promise<any>) {
      this.promise = promise;
      this.promise
        .then(data => {
          this.data = data;
        })
        .catch(error => {
          this.error = error;
        })
        .finally(() => {
          this.promise = null;
        });
    }
  }));

  return dataStore;
};

export default useDataPromise;