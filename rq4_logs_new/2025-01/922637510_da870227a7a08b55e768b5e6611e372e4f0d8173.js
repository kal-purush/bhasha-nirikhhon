```javascript
import { AbortController } from 'node-abort-controller';

export default async function handler(req, res) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 5000); // 5 seconds timeout

  try {
    const response = await Promise.race([
      new Promise((resolve) => {
        setTimeout(() => {
          resolve({ message: 'Data fetched successfully' });
        }, 3000); // Simulate a longer operation
      }),
      new Promise((_, reject) => {
        controller.signal.addEventListener('abort', () =>
          reject(new Error('Request timed out'))
        );
      }),
    ]);
    clearTimeout(timeoutId);
    res.status(200).json(response);
  } catch (error) {
    clearTimeout(timeoutId);
    console.error('Error:', error);
    res.status(error.message.includes('timeout') ? 408 : 500).json({ error: error.message });
  }
}
```