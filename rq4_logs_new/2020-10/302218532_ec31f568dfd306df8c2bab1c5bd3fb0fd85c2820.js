import Head from 'next/head';
import Navbar from '../Navbar';

export default () => (
  <div>
    <Head>
      <title>Dreamers</title>
      <meta name="viewport" content="initial-scale=1.0, width=device-width" />
      <meta httpEquiv="X-UA-Compatible" content="ie=edge" />
      <meta charSet="UTF-8" />
      <meta name="author" content="Michael Schmidgall" />
      <meta
        name="keywords"
        content="Dreamers, community, writing prompts, prompts, how to get better at writing"
      />
      <meta
        name="description"
        content="Dreamers is the premier writing resource and communication tool for authors and avid readers."
      />
     <link
      rel="stylesheet"
      href="https://stackpath.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css"
    />
    <link
      rel="stylesheet"
      href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/3.7.2/animate.css"
    />
   <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css" integrity="sha384-JcKb8q3iqJ61gNV9KGb8thSsNjpSL0n8PARn9HuZOnIxN0hoP+VmmDGMN5t9UJ0Z" crossorigin="anonymous" />
      <script
        src="https://code.jquery.com/jquery-3.3.1.js"
        integrity="sha256-2Kok7MbOyxpgUVvAk/HJ2jigOSYS2auK4Pfzbm7uH60="
        crossOrigin="anonymous"
      />
      <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/js/bootstrap.min.js" />
    </Head>
    <Navbar />
  </div>
);