const express=require('express');
const path=require('path');

const app=express();

let initial_path = path.join(__dirname, "public");
app.use(express.static(initial_path));

app.get("/", (req, res) => {
  res.sendFile(path.join(initial_path, "index.html"));
});
const PORT = process.env.PORT || 5001;
app.listen(PORT, () => console.log(`App iniciado en ${PORT}`));