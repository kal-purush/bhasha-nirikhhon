export const API_ROOT = 'https://www.reddit.com';

export const getSubredditPosts = async (subreddit) => {
    const response = await fetch(`${API_ROOT}${subreddit}.json`);
    console.log('API Response:', response); 
    const json = await response.json();
    console.log('Parsed JSON:', json);
    return json.data.children.map((post) => post.data);
  };
  
  export const getSubreddits = async () => {
    const response = await fetch(`${API_ROOT}/subreddits.json`);

   console.log('API Response:', response); 
    const json = await response.json();
    console.log('Parsed JSON:', json);
  
    return json.data.children.map((subreddit) => subreddit.data);
  };
  
  export const getPostComments = async (permalink) => {
    const response = await fetch(`${API_ROOT}${permalink}.json`);

   console.log('API Response:', response); 
    const json = await response.json();
    console.log('Parsed JSON:', json);
  
    return json[1].data.children.map((subreddit) => subreddit.data);
  };