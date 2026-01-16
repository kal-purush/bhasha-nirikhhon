const { storage } = require("firebase-admin");

exports.uploadPhotoHandler = async (req, res) => {
  try {
    // console.log(req.body.input)
    const { base64Image } = req.body.input;
    const contentType = base64Image.match(
      /data:([a-zA-Z0-9]+\/[a-zA-Z0-9-.+]+).*,.*/
    )[1];
    const file = storage().bucket().file(`photos/${Date.now()}`);

    const base64EncodedImageString = base64Image.replace(
      /data:image\/\w+;base64,/,
      ""
    );
    const imageBuffer = new Buffer(base64EncodedImageString, "base64");
    await file.save(imageBuffer, { contentType });

    const url = await file.getSignedUrl({
      action: "read",
      expires: new Date(Date.now() + 1000 * 60 * 60 * 24 * 365 * 10),
    });

    res.status(200).send({ url });
  } catch (error) {
    console.log(error);
    res.status(500).send({ message: error.message });
  }
};