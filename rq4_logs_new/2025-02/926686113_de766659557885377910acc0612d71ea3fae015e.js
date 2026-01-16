// error middle ware || nextfunction
const erroMiddleware = (err, req, res, next) => {
    console.log(err);
    res.status(500).send({
        success:"false",
        message: "Internal Server Error",
        err,
    });
}
export default erroMiddleware;