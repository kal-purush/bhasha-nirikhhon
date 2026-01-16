
import { PUBLIC_BACKEND_URL } from '$env/static/public';

const getTemplate = async (templateName) => {
  const apiUrl = new URL(`${PUBLIC_BACKEND_URL}/api/tools/templates/og-image?template=${templateName}`);

  try {
    const response = await fetch(apiUrl, {
      method: 'GET',
      headers: {
        'Content-Type': 'text/html',
      },
    });
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return response.text();
  } catch (error) {
    return null;
  }
}

export { getTemplate };