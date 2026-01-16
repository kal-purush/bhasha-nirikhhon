interface Env {
    ASSETS: Fetcher;
  }
  
  export default {
    fetch(request, env) {
      const url = new URL(request.url);
  
      if (url.pathname.startsWith("/api/events")) {
        return Response.json([
          {
            id: '1',
            name: "event 1",
            place: "pandaan",
            time: 1741247092,
          },
          {
            id: '2',
            name: "event 2",
            place: "sukorejo",
            time: 1741246092,
          },
          {
            id: '3',
            name: "event 3",
            place: "purwosari",
            time: 1741245092,
          },
        ]);
      }
  
      return env.ASSETS.fetch(request);
    },
  } satisfies ExportedHandler<Env>;