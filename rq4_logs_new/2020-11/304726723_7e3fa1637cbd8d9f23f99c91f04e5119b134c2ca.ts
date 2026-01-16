import mainDB from "./util";

// Image architecture
export interface Image {
  id: string;
  data: Buffer;
  registerTime: number;
}

// Image services
export module ImageService {
  // Delete an image
  export async function deleteImage(id: string): Promise<void> {
    const sql = `DELETE FROM Image WHERE id = ?;`;
    const params = [id];
    await mainDB.execute(sql, params);
  }
}