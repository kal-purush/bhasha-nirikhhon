
export async function postImageUrl(imageUrl) {
    const response = await fetch(`http://localhost:9093/image/upload?url=${imageUrl}`, {
        mode: 'no-cors',
        method: 'POST',
        headers: {'Content-Type': 'application/json'}
      })
    return response.ok;
}