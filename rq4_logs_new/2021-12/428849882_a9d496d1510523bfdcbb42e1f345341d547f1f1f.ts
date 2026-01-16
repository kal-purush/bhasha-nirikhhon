import { NextApiRequest, NextApiResponse } from "next";
import { doc, getDoc, updateDoc, setDoc } from "firebase/firestore";
import { db } from "../../../firebase";

export default async function profileHandler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const {
    query: { wallet },
    method,
  } = req as any;

  switch (method) {
    case "GET":
      const docRef = doc(db, "users", wallet);
      const docSnap = await getDoc(docRef);

      if (docSnap.exists()) {
        var profile_username = docSnap.data().profile_username;
        var profile_image = docSnap.data().profile_image;
        var profile_bio = docSnap.data().profile_bio;
      } else {
        var message: any = "No document!";
        var profile_username: any = "No profile";
        var profile_image: any = "/images/content/no-user.jpeg";
      }

      res.status(200).json({
        req: wallet,
        profile_username,
        profile_image,
        profile_bio,
        message,
      });
      break;

    case "PUT":
      const updateRef = doc(db, "users", wallet);

      await setDoc(
        updateRef,
        {
          profile_username: req.body.profile_username,
          profile_bio: req.body.profile_bio,
        },
        { merge: true }
      );

      res.status(200).json({ status: "update success!" });
      break;

    default:
      res.setHeader("Allow", ["GET", "PUT"]);
      res.status(405).end(`Method ${method} Not Allowed`);
  }
}