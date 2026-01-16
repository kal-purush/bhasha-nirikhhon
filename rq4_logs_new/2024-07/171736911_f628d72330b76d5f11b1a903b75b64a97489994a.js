module.exports = function () {
    const nickname = 'tom';
    const twtxtUrl = 'https://tomcasavant.com/twtxt.txt';
  
    const posts = [
      `2024-07-08T13:50:00Z /nick ${nickname}`,
      `2024-07-08T13:51:00Z /twturl ${twtxtUrl}`,
      `2024-07-08T13:52:00Z Hello world!`,
    ];
  
    const twtxtContent = posts.join('\n');
  
    return {
      content: twtxtContent,
    };
  };
  