const generateUrlPath = (path) => {
    const basePath = import.meta.env.BASE_URL;
  
    // Remove the first character if it is '/'
    if (path.startsWith('/')) {
      path = path.substring(1);
    }
  
    if (path === '/') {
      return basePath;
    }
  
    return basePath + path;
  };
  


export {
    generateUrlPath
}