import { NextRouter } from "next/router";
import { TransactionMode } from "remultiform/database";
import { processStoreNames } from "../storage/ProcessDatabaseSchema";
import Storage from "../storage/Storage";
import basePath from "./basePath";
import getProcessApiJwt from "./getProcessApiJwt";
import getProcessRef from "./getProcessRef";

const persistProcessData = async (
  router: NextRouter,
  setProgress?: (progress: number) => void
): Promise<void> => {
  let progress = 0;

  if (setProgress) {
    setProgress(progress);
  }

  if (Storage.ProcessContext && Storage.ProcessContext.database) {
    const processRef = getProcessRef(router);
    const processApiJwt = getProcessApiJwt(processRef);

    if (!processRef || !processApiJwt) {
      console.error(
        "Unable to persist process data due to missing session data"
      );

      return;
    }

    const json = await Storage.getProcessJson(processRef);

    if (json) {
      const { processJson, imagesJson } = json;

      const progressIncrement = 1 / (imagesJson.length + 2);

      await Promise.all(
        imagesJson.map(async ({ id, image }) => {
          const response = await fetch(
            `${basePath}/api/v1/processes/${processRef}/images?jwt=${processApiJwt}`,
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json"
              },
              body: JSON.stringify({ id, image })
            }
          );

          if (!response.ok) {
            throw new Error(`${response.status}: ${response.statusText}`);
          }

          progress += progressIncrement;

          if (setProgress) {
            setProgress(progress);
          }
        })
      );

      const response = await fetch(
        `${basePath}/api/v1/processes/${processRef}/processData?jwt=${processApiJwt}`,
        {
          method: "PATCH",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify(processJson)
        }
      );

      if (!response.ok) {
        throw new Error(`${response.status}: ${response.statusText}`);
      }

      progress += progressIncrement;

      if (setProgress) {
        setProgress(progress);
      }

      const db = await Storage.ProcessContext.database;

      // To reduce risk of data loss, we only clear up the data if we sent
      // something to the backend.
      await db.transaction(
        processStoreNames,
        async stores => {
          await Promise.all(
            Object.values(stores).map(store => store.delete(processRef))
          );
        },
        TransactionMode.ReadWrite
      );

      sessionStorage.clear();

      if (setProgress) {
        setProgress(1);
      }
    }
  } else {
    console.warn("No process data to persist");
  }
};

export default persistProcessData;