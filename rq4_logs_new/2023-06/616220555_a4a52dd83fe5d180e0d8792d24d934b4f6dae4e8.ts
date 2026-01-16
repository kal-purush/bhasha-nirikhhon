export const getContentUrl = ({
  id, 
  type, 
  seasonNumber, 
  episodeNumber
}: {
  id: string, 
  type: string, 
  seasonNumber: string, 
  episodeNumber: string
}): string => {
  let url = "https://api.themoviedb.org/3";
  switch (type) {
    case "tv":
      url = `${url}/${type}/${id}`;
      break;
    case "movie":
      url = `${url}/${type}/${id}`;
      break;
    case "season":
      url = `${url}/tv/${id}/${type}/${seasonNumber}`;
      break;
    case "episode":
      url = `${url}/tv/${id}/season/${seasonNumber}/${type}/${episodeNumber}`;
      break;
    default:
  }
  return url;
}

export const getContentRuntime = (content: any, type: string) => {
  let totalRuntime = 0;
  switch (type) {
    case "movie":
    case "episode":
      totalRuntime += content.runtime;
      break;
    case "season":
      totalRuntime = content.episodes.reduce(( sum: number, { runtime } : { runtime: number } ) => sum + runtime , totalRuntime);
      break;
    default:
  }
  return totalRuntime;
}

export const getContentAmount = (content: any, type: string) => {
  return type === 'season' ? content.episodes.length : 1;
}