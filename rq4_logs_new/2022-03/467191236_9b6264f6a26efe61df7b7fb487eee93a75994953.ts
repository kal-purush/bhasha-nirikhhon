import models from "../../../lib/models";


export default async (req, res) => {
  const data = req.body;
  try {
    const result = await models.post.findMany();
    res.status(200).json(result);
  } catch (err) {
    console.log(err);
    res.status(403).json({ err: "Error occured." });
  }
};