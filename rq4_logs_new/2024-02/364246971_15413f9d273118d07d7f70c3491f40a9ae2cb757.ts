// SPDX-FileCopyrightText: 2023-2024 Tampere region
//
// SPDX-License-Identifier: LGPL-2.1-or-later

import { S3Client } from "@aws-sdk/client-s3";
import { createPresignedPost } from "@aws-sdk/s3-presigned-post";

const bucketName = process.argv[2];
if (bucketName === undefined) {
    console.error("Usage: npm run --silent generate-upload-form [bucket-name]");
    process.exit(1);
}

createPresignedPost(new S3Client({ region: "eu-west-1" }), {
    Bucket: bucketName,
    Key: "${filename}",
    Expires: 60 * 60 * 24 * 7,
    Fields: { success_action_status: "200" },
    Conditions: [{ success_action_status: "200" }],
})
    .then(({ url, fields }) =>
        process.stdout.write(
            `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title></title>
  </head>
  <body>
    <h1>Aineiston siirto eVakaan</h1>
    <form action="${url}" method="post" enctype="multipart/form-data">
      ${Object.entries(fields)
          .map(([k, v]) => `<input type="hidden" name="${k}" value="${v}">`)
          .join("\n")}
      Tiedoston valinta:<br>
      <input type="file" name="file"><br>
      <br>
      Tiedoston lähetys:<br>
      <input type="submit" name="submit" value="Lataa tiedosto eVakaan">
      <p>Tyhjä sivu napin painalluksen jälkeen = lataus onnistui</p>
    </form>
  </body>
</html>
`,
        ),
    )
    .catch((err) => {
        console.error(err);
        process.exit(1);
    });