import { NextApiRequest, NextApiResponse } from 'next';
import formidable from 'formidable';
import { uploadFileToS3, deleteFileFromS3 } from '@/lib/s3';

export const config = {
  api: {
    bodyParser: false,  // Next.js의 기본 바디 파서를 비활성화합니다.
  },
};

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const form = formidable({ multiples: true });

  form.parse(req, async (err, fields, files) => {
    if (err) {
      console.error('Formidable error:', err);
      return res.status(500).json({ message: 'File upload error' });
    }

    try {
      let file: formidable.File | undefined;

      if (Array.isArray(files.menuImage)) {
        file = files.menuImage[0];
      } else {
        file = files.menuImage as formidable.File | undefined;
      }

      if (!file) {
        return res.status(400).json({ message: 'No file uploaded' });
      }

      const s3Url = await uploadFileToS3(file);
      return res.status(200).json({ imageUrl: s3Url });
    } catch (error) {
      console.error('Error uploading to S3:', error);
      return res.status(500).json({ message: 'S3 upload error' });
    }
  });
}