import React, { useEffect, createRef } from 'react';

const src = 'https://utteranc.es/client.js';

export default ({ repo }) => {
  const containerRef = createRef();
  useEffect(() => {
    const utterances = document.createElement('script');
    const attributes = {
      src,
      repo,
      'issue-term': 'pathname',
      label: '💬',
      theme: 'github-light',
      crossorigin: 'anonymous',
      async: true,
    };
    Object.entries(attributes).forEach(([key, value]) => {
      utterances.setAttribute(key, value);
    });
    containerRef.current.appendChild(utterances);
  }, [repo, containerRef]);
  return <div style={{ marginTop: '3rem' }} ref={containerRef} />;
};
//   <script
//   src="https://utteranc.es/client.js"
//   repo="billowycloud/SpicyCookie.io"
//   issue-term="pathname"
//   label="💬"
//   theme="github-light"
//   crossorigin="anonymous"
//   async
// ></script>;