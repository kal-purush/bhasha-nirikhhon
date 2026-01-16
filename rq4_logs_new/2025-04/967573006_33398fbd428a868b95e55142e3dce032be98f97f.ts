// api.test.ts
import { fetchImages } from '../api';

describe('fetchImages', () => {
  const mockData = [{
    author : "Alejandro Escamilla",
    download_url: "https://picsum.photos/id/0/5000/3333",
    height: 3333,
    id: "0",
    url: "https://unsplash.com/photos/yC-Yzbqy7PY",
    width: 5000 }];

  beforeEach(() => {
    global.fetch = jest.fn();
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  it('should fetch images and return JSON data', async () => {
    (fetch as jest.Mock).mockResolvedValueOnce({
      json: jest.fn().mockResolvedValueOnce(mockData),
    });

    const result = await fetchImages(1, 10);
    expect(fetch).toHaveBeenCalledWith('https://picsum.photos/v2/list?page=1&limit=10');
    expect(result).toEqual(mockData);
    expect(fetch).toHaveBeenCalledWith(
        expect.stringContaining('page=1&limit=10')
    );
  });


  it('should return an empty array if fetch fails', async () => {
    (fetch as jest.Mock).mockRejectedValueOnce(new Error('Network error'));

    const result = await fetchImages(1, 10);
    expect(result).toEqual([]);
  });
});