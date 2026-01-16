import bcrypt from "bcrypt";
import MongoStore from "connect-mongo";
import cors from "cors";
import dotenv from "dotenv";
import express from "express";
import session from "express-session";
import mongodb, { MongoClient, ServerApiVersion } from "mongodb";
import passport from "passport";
import { Strategy as LocalStrategy } from "passport-local";
import { UserDocument } from "./types";

dotenv.config();

const PORT = process.env.PORT || 8080;

if (
  process.env.MONGODB_URI === undefined ||
  process.env.SESSION_SECRET === undefined
) {
  throw new Error("dotenv is not configured!");
}

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(process.env.MONGODB_URI, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  },
});

(async function init() {
  try {
    // Connect the client to the server	(optional starting in v4.7)
    await client.connect();
    // Send a ping to confirm a successful connection
    await client.db("main-db").command({ ping: 1 });
    console.log("Successfully connected to MongoDB!");
  } catch (error) {
    console.error(error);
  }
})();

passport.use(
  new LocalStrategy(async (email, password, done) => {
    try {
      const users = client.db("main-db").collection("users");
      const userDocument = await users.findOne({ email });

      // check if user exists
      if (userDocument === null) {
        return done(null, false);
      }

      // check for correct password
      const isPasswordMatch = await bcrypt.compare(
        password,
        userDocument.password
      );

      if (isPasswordMatch) {
        return done(null, userDocument);
      }

      // wrong password
      return done(null, false);
    } catch (error) {
      return done(error, false);
    }
  })
);

// convert {expressUser / userDocument} to id
passport.serializeUser((expressUser, done) => {
  const userDocument = expressUser as UserDocument;
  return done(null, userDocument._id);
});

// convert id to {expressUser / userDocument}
passport.deserializeUser(async (_id: string, done) => {
  try {
    const users = client.db("main-db").collection("users");
    const userDocument = await users.findOne({
      _id: new mongodb.ObjectId(_id),
    });

    if (userDocument === null) {
      return done(`User with _id="${_id}" does not exist`, false);
    }

    return done(null, userDocument);
  } catch (error) {
    return done(error, false);
  }
});

const app = express();
app.use(cors());
app.use(express.json());

// express-session middleware must be set up before passport middleware
app.use(
  session({
    secret: process.env.SESSION_SECRET,
    resave: false,
    saveUninitialized: true,
    cookie: { secure: true },
    store: MongoStore.create({
      mongoUrl: process.env.MONGODB_URI,
      dbName: "main-db",
    }),
  })
);

app.use(passport.initialize());
app.use(passport.session());

app.get("/", (req, res) => {
  res.send("rainbow");
});

app.post("/auth/sign-up", async (req, res) => {
  try {
    const { email, password } = req.body;

    // check whether this email already exists
    const users = client.db("main-db").collection("users");
    const userDocument = await users.findOne({ email });

    if (userDocument) {
      // this email already exists
      res.status(409).send("Duplicated email");
      return;
    }

    const hashedPassword = await bcrypt.hash(password, 10);
    const newUserDocument = {
      email,
      password: hashedPassword,
    };

    const result = await users.insertOne(newUserDocument);

    if (result.acknowledged) {
      res.status(200).send();
    } else {
      throw new Error("Database failed to acknowledge request");
    }
  } catch (error) {
    console.error(error);
    res.status(503).send(error);
  }
});

app.get("/secret", passport.authenticate("local"), (req, res) => {
  res.send("secret");
});

app.listen(PORT, () => {
  console.log(`Listening on http://localhost:${PORT}`);
});