import { GetParticipateResult, MainClass, GetOption } from "@/model/post";

export function convertResult(
  allResults: GetParticipateResult[],
  mainClass: MainClass,
  subClass: string,
  options: GetOption[]
) {
  let data = allResults.filter((result) => {
    return result[mainClass] == subClass;
  });
  data = data.map((v, i) => ({ ...v, index: `${i + 1}번` }));
  data.sort((a, b) => -a.count + b.count);
  data = data?.map((v) => {
    const nv = options.find((o) => o.id === v.optionId);
    return { ...v, value: nv!.value };
  });
  return data;
}