import express, { Request, Response } from "express";
import cors from "cors";
import { v4 as generateId } from "uuid";
import { AddressInfo } from "net";
import { type } from "os";

const app = express();
app.use(express.json());


// Ex1
app.get("/ping", (req, res) => {          
  res.send("Pong! 🏓")
})

// Ex2
type USERTIPE = {
  userId: string,
  id: any,
  title: string,
  completed: boolean
}

// Ex3
const afazeres: USERTIPE[] = [
  {
    userId: generateId(),
    id: generateId(),
    title: "titulo 1",
    completed: true
  },
  {
    userId: generateId(),
    id: generateId(),
    title: "titulo 2",
    completed: false
  },
  {
    userId: generateId(),
    id: generateId(),
    title: "titulo 3",
    completed: true
  },
  {
    userId: generateId(),
    id: generateId(),
    title: "titulo 4",
    completed: true
  },
]

// Ex4
app.get("/afazeres/completos", (req: Request, res: Response) => {
  try {    
    const user = afazeres.filter((w) => w.completed === true);

    if (!user) throw new Error("Tarefa não feita.");
    res.send(user);

  } catch (err) {
    res.send(err);
  }
});

app.get("/afazeres/naocompletos", (req: Request, res: Response) => {
  try {    
    const user = afazeres.filter((w) => w.completed === false);

    if (!user) throw new Error("Tarefa feita.");
    res.send(user);

  } catch (err) {
    res.send(err);
  }
});

// Ex5
app.post("/afazeres/criacao", (req:Request , res: Response)=>{
  try {    
    const userTitle = req.headers.authorization;
    const user = afazeres.find((w:any) => w.title === userTitle);
    if (!user) throw new Error("Authorization errado");

    const atividadeName = req.body.title;
    if (!atividadeName)
      throw new Error("É necessário informar o nome da nova atividade");

       const afazer:any = user.title.find(
      (w:any) => w.title === (atividadeName as string).toLowerCase()
    );
    if (afazeres) throw new Error("Essa atividade já existe");

    const novoAfazer = {
      userId: generateId(),
      id: generateId(),
      title: atividadeName,
      completed: true || false
    }

    afazeres.push(novoAfazer);
    res.status(201).send("Tarefa criada com sucesso");
  } catch (err) {
    res.send(err);
  }
})

//Ex6

//Ex7

//Ex8

//Ex9

const server = app.listen(process.env.PORT || 3003, () => {
  if (server) {
    const address = server.address() as AddressInfo;
    console.log(`Server is running in http://localhost:${address.port}`);
  } else {
    console.error(`Failure upon starting server.`);
  }
});;