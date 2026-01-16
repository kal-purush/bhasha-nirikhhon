const dbName = "graphDatabase";
const storeName = "graphs";

const openIndexedDB = () => {
  return new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(dbName, 1);

    request.onupgradeneeded = (event) => {
      const db = request.result;

      if (!db.objectStoreNames.contains(storeName)) {
        db.createObjectStore(storeName, { keyPath: "name" });
      }
    };

    request.onsuccess = () => {
      resolve(request.result);
    };

    request.onerror = () => {
      reject(request.error);
    };
  });
};

export const addGraph = async (graphName: string, graphData: any) => {
  const db = await openIndexedDB();
  return new Promise<void>((resolve, reject) => {
    const transaction = db.transaction([storeName], "readwrite");
    const store = transaction.objectStore(storeName);
    const request = store.put({ name: graphName, data: graphData });

    request.onsuccess = () => {
      resolve();
    };

    request.onerror = () => {
      reject(request.error);
    };
  });
};

export const getGraph = async (graphName: string) => {
  const db = await openIndexedDB();
  return new Promise<any>((resolve, reject) => {
    const transaction = db.transaction([storeName], "readonly");
    const store = transaction.objectStore(storeName);
    const request = store.get(graphName);

    request.onsuccess = () => {
      resolve(request.result?.data);
    };

    request.onerror = () => {
      reject(request.error);
    };
  });
};

export const deleteGraph = async (graphName: string) => {
  const db = await openIndexedDB();
  return new Promise<void>((resolve, reject) => {
    const transaction = db.transaction([storeName], "readwrite");
    const store = transaction.objectStore(storeName);
    const request = store.delete(graphName);

    request.onsuccess = () => {
      resolve();
    };

    request.onerror = () => {
      reject(request.error);
    };
  });
};

export const renameGraph = async (oldName: string, newName: string) => {
  const db = await openIndexedDB();
  const graphData = await getGraph(oldName);
  if (graphData) {
    await addGraph(newName, graphData);
    await deleteGraph(oldName);
  }
};

export const getAllGraphs = async () => {
  const db = await openIndexedDB();
  return new Promise<any[]>((resolve, reject) => {
    const transaction = db.transaction([storeName], "readonly");
    const store = transaction.objectStore(storeName);
    const request = store.getAll();

    request.onsuccess = () => {
      resolve(request.result.map((item: any) => item.data));
    };

    request.onerror = () => {
      reject(request.error);
    };
  });
};