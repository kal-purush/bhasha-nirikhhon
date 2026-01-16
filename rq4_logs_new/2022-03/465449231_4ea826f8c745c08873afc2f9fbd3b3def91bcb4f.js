// pages/_document.js

import { Html, Head, Main, NextScript } from "next/document";

export default function Document() {
  return (
    <Html>
      <Head>
        {/* <link href="https://fonts.googleapis.com/css2?family=Inter&display=optional" rel="stylesheet" /> */}
        {/* Prevent Layout Shifting due/Layout Janking to FOUT and FOIT **:rel="preload" display=optional */}
        <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=optional" rel="preload" as="font" />
      </Head>
      <body>
        <Main />
        <NextScript />
      </body>
    </Html>
  );
}