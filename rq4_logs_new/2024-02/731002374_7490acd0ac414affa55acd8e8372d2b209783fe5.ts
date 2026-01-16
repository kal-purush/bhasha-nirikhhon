import type { NextApiRequest,  NextApiResponse } from 'next';

type ApiResponse = {
  name: string;
}

const githubToken = process.env.NEXT_PUBLIC_GITHUB_TOKEN!;

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if ( req.method === 'POST' ) {
    const { args } = req.body;

    const url = 'https://api.github.com/repos/Krostar5793/itky-quest/actions/workflows/.yml/dispatches';

    const headers = {
      'Authorization': `Bearer ${githubToken}`,
      'Accept': 'application/vnd.github.v3+json',
    };
    const body = {
      ref: 'main',
      inputs: { args },
    };

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: headers,
        body: JSON.stringify(body),
      });

      if ( response.ok ) res.status(200).json({ message: 'Workflow triggered successfully' });
      else res.status(response.status).json({ message: 'Failed to trigger workflow' });

    } catch ( e ) {
      console.error('Error triggering workflow:', e);
      res.status(500).json({ message: 'Internal Server Error' });
    }
  } else {
    res.setHeader('Allow', ['POST']);
    res.status(405).json({ message: `Method ${req.method} Not Allowed` });
  }
}