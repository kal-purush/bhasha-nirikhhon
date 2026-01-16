import type { MetadataRoute } from "next";

// Next.js PWA docs:
// https://nextjs.org/docs/app/building-your-application/configuring/progressive-web-apps
export default function manifest(): MetadataRoute.Manifest {
  return {
    name: "Gate Controller Cloud V3",
    short_name: "GateController",
    description: "An app to control your gate",
    start_url: "/dashboard",
    display: "standalone",
    background_color: "#000000",
    theme_color: "#121212",
    icons: [
      {
        src: "icon-192x192.png",
        sizes: "192x192",
        type: "image/png",
      },
      {
        src: "icon-512x512.png",
        sizes: "512x512",
        type: "image/png",
      },
      {
        src: "icon-192x192-maskable.png",
        sizes: "192x192",
        type: "image/png",
        purpose: "maskable",
      },
      {
        src: "icon-512x512-maskable.png",
        sizes: "512x512",
        type: "image/png",
        purpose: "maskable",
      },
    ],
  };
}