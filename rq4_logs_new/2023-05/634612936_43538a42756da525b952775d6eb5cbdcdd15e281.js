import axios from "axios";
import { useState } from "react";

function App() {
  const [URL, setURL] = useState("");
  const [siteObj, setSiteObj] = useState({});
  const [isLoading, setIsLoading] = useState(false);
  const submitHandler = async (event) => {
    event.preventDefault();
    setIsLoading(true);
    const response = await axios.get(`http://localhost:5000/?url=${URL}`);
    // console.log(response.data);
    setURL(Object.keys(response.data)[0]);
    setSiteObj(response.data);
    setIsLoading(false);
  };

  const generateSiteMap = (siteObj, url, parentUrl) => {
    // console.log(siteObj[url], url);
    // console.log("Checking", parentUrl, url);
    return (
      <div>
        <div>
          {url === parentUrl
            ? url
            : url.includes(parentUrl)
            ? "- " + url.slice(parentUrl.length + 1)
            : "- " + url}
        </div>
        <div className="pl-5">
          {siteObj[url].map((currentURL) =>
            generateSiteMap(siteObj, currentURL, url)
          )}
        </div>
      </div>
    );
  };

  return (
    <div className="flex-row h-screen w-screen">
      <div className="text-center w-1/2 mx-auto py-10 font-extrabold underline text-3xl">
        Web Crawler
      </div>
      <div className="text-center w-1/2 mx-auto py-10">
        <div className="bg-gray-200 rounded  flex-col">
          <form
            className="p-4 w-5/6 mx-auto py-10 flex text-sm"
            onSubmit={submitHandler}
          >
            <input
              placeholder="Enter URL"
              onChange={(e) => {
                setURL(e.target.value);
                setSiteObj({});
              }}
              value={URL}
              className="text-center w-full placeholder-black rounded-l py-2"
            />
            <button className="bg-green-200 rounded-r px-4 flex py-2">
              {isLoading && (
                <div role="status">
                  <svg
                    aria-hidden="true"
                    class="w-5 h-5 mr-3 text-red-200 animate-spin dark:text-blue-600 fill-black"
                    viewBox="0 0 100 101"
                    fill="none"
                    xmlns="http://www.w3.org/2000/svg"
                  >
                    <path
                      d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z"
                      fill="currentColor"
                    />
                    <path
                      d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z"
                      fill="currentFill"
                    />
                  </svg>
                  <span class="sr-only">Loading...</span>
                </div>
              )}
              <div className="my-auto">
                {isLoading ? "Crawling..." : "Crawl"}
              </div>
            </button>
          </form>
          <div className="bg-gray-400 rounded-b">
            <div className="pt-10 underline text-xl pb-5">Instructions</div>
            <div className="w-2/3 mx-auto pb-10">
              Enter URL in the above URL field & the Web Crawler will crawl the
              specified website & return a sitemap
            </div>
          </div>
        </div>
      </div>
      {Object.keys(siteObj).length !== 0 && (
        <div className="mx-auto w-1/2 bg-gray-200 px-5 py-5 rounded">
          {generateSiteMap(siteObj, URL, URL)}
        </div>
      )}
    </div>
  );
}

export default App;