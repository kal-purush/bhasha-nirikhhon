import exp from "express";
const PORT = 3000;

const app = exp();

app.get("/", (req,res) => res.json({status: "NTask API"}));
app.listen(PORT,() => console.log(`NTask API - porta ${PORT}`));