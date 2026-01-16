import mainDB from "./util";

// Location type architecture
export interface LocationType {
  id: number;
  name: string;
}

// Location type services
export module LocationTypeService {
  // Get all locations
  export async function getLocations(): Promise<LocationType[]> {
    const sql = `SELECT id, name FROM LocationType ORDER BY id;`;
    const rows: LocationType[] = await mainDB.execute(sql);
    return rows;
  }

  // Get the name of a location by ID
  export async function getLocationName(locationID: number): Promise<string> {
    const sql = `SELECT name FROM LocationType WHERE id = ?;`;
    const params = [locationID];
    const rows: LocationType[] = await mainDB.execute(sql, params);
    return rows[0].name;
  }

  // Check if a location is valid
  export async function validLocation(locationID: number): Promise<boolean> {
    const sql = `SELECT id FROM LocationType WHERE id = ?;`;
    const params = [locationID];
    const rows: LocationType[] = await mainDB.execute(sql, params);
    return rows.length > 0;
  }
}