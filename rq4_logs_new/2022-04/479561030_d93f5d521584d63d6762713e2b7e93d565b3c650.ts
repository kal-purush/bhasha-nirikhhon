export async function getPrefectures(){
  const url = new URL("https://opendata.resas-portal.go.jp/api/v1/prefectures");
  const result = await fetch(url.toString(),{
    method: 'GET',
    cache: "default",
    headers: {
      "X-API-KEY": String(process.env.RESAS_API_KEY)
    }
  }).then(res => res.json());
  return result;
}

export async function getPopulationDetail(prefecture: number){
  const url = new URL("https://opendata.resas-portal.go.jp/api/v1/population/composition/perYear");
  url.searchParams.set("prefCode", prefecture.toString());
  url.searchParams.set("cityCode", '-');
  const result = await fetch(url,{
    method: 'GET',
    cache: "default",
    headers: {
      "X-API-KEY": String(process.env.RESAS_API_KEY)
    }
  }).then(res => res.json());
  return result;
}