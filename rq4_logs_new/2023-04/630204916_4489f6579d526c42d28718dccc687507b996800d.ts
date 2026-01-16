import { Router } from "express";
import { loginUser } from "../../controllers/inputs/login/loginUser";
import { loginChauffeur } from "../../controllers/inputs/login/loginChauffeur";
import { loginDistrutor } from "../../controllers/inputs/login/loginDistrutor";
import { loginLocal } from "../../controllers/inputs/login/loginLocal";


const router = Router();

router.post("/user", loginUser);

// router.post("/chauffeur", loginChauffeur);

// router.post("/distrutor", loginDistrutor);

// router.post("/local", loginLocal);

export default router;