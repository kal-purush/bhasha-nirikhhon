'use server';
export async function getVideoList(page = 0, itemsPerPage = 50, orderBy = 'date') {
  const url = `https://video.bunnycdn.com/library/${process.env.BUNNYCDN_STREAM_LIB_ID}/videos?page=1&itemsPerPage=100&orderBy=date`;
  const options = {
    method: 'GET',
    headers: {
      accept: 'application/json',
      AccessKey: `${process.env.BUNNYCDN_STREAM_KEY}`,
    },
  };

  try {
    const response = await fetch(url, options);
    return response.json();
  } catch (error) {
    console.error('Error getting video list', error);
    return { message: 'Error getting video list' };
  }
}