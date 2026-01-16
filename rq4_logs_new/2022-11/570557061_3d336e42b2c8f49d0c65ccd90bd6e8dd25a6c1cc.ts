export default defineConfig({
  //...
  token: process.env.NEXT_PUBLIC_TINA_TOKEN // generated on app.tina.io,
  clientId: process.env.NEXT_PUBLIC_TINA_CLIENT_ID, // generated on app.tina.io
  branch,
  schema: {
    collections: [
    //...
    // See https://tina.io/docs/schema/ for more info about "collections"
  ]}
})