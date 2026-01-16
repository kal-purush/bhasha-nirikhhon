import Elysia from "elysia";

new Elysia()
    .get('/', ({ headers }) => headers.host)
    .listen(3000, () => {
        console.log('placeholder running');
    });