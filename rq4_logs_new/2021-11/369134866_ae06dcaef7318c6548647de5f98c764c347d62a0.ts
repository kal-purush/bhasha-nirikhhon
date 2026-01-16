import { camelCase } from "./Utils.api";

export async function getCancerHierarchy(): Promise<any> {
  let response = await fetch(
    `${process.env.REACT_APP_API_URL}/models_by_cancer`
  );
  if (!response.ok) {
    throw new Error("Network response was not ok");
  }
  return response.json().then((d) => {
    let hierarchy: any = {};
    d.filter((i: any) => i.cancer_system !== null).forEach((element: any) => {
      if (hierarchy[element.cancer_system] === undefined) {
        hierarchy[element.cancer_system] = {
          name: element.cancer_system,
          children: [],
        };
      }
      hierarchy[element.cancer_system].children.push({
        name: element.histology,
        count: element.count,
      });
    });
    return { name: "PDCM Models", children: Object.values(hierarchy) };
  });
}

export async function getFrequentlyMutatedGenes() {
  let response = await fetch(
    `${process.env.REACT_APP_API_URL}/models_by_mutated_gene?order=count.desc&limit=20`
  );
  if (!response.ok) {
    throw new Error("Network response was not ok");
  }
  return response
    .json()
    .then((d: Array<any>) => d.reverse().map((i: any) => camelCase(i)));
}

export async function getModelsByDatasetAvailability() {
  let response = await fetch(
    `${process.env.REACT_APP_API_URL}/models_by_dataset_availability?order=count.desc`
  );
  if (!response.ok) {
    throw new Error("Network response was not ok");
  }
  return response.json().then((d: Array<any>) =>
    d.reverse().map((i: any) => {
      return {
        id: i.dataset_availability,
        label: i.dataset_availability,
        value: i.count,
      };
    })
  );
}