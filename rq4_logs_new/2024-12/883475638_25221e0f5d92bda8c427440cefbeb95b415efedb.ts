import { pool } from "../src/database/CarmineDB";

export async function testQueries() {
  const client = await pool.connect();
  try {
    console.log("Starting query tests...");

    // Begin transaction
    await client.query("BEGIN");

    /** Test 1: Retrieve all users */
    console.time("Retrieve all users");
    const allUsersResult = await client.query("SELECT * FROM users");
    console.timeEnd("Retrieve all users");
    console.log(`Number of users retrieved: ${allUsersResult.rowCount}`);

    /** Test 2: Retrieve user by ID */
    const testUserId = "5fd496ec-16fd-423c-99b5-ed59796ae0eb"; // Replace with a valid UUID
    console.time("Retrieve user by ID");
    const userByIdResult = await client.query("SELECT * FROM users WHERE user_id = $1", [testUserId]);
    console.timeEnd("Retrieve user by ID");
    console.log(`User retrieved by ID:`, userByIdResult.rows);

    /** Test 3: Search users by name (partial match) */
    const searchName = "Luke";
    console.time("Search users by name");
    const usersByNameResult = await client.query("SELECT * FROM users WHERE user_name ILIKE $1", [`%${searchName}%`]);
    console.timeEnd("Search users by name");
    console.log(`Users retrieved by name:`, usersByNameResult.rows);

    /** Test 4: Update user email */
    const newEmail = "carmine12@gmail.con";
    console.time("Update user email");
    const updateEmailResult = await client.query(
      "UPDATE users SET user_email = $1 WHERE user_id = $2 RETURNING *",
      [newEmail, testUserId]
    );
    console.timeEnd("Update user email");
    console.log(`User email updated:`, updateEmailResult.rows);

    /** Test 5: Delete user */
    console.time("Delete user");
    const deleteUserResult = await client.query("DELETE FROM users WHERE user_id = $1 RETURNING *", [testUserId]);
    console.timeEnd("Delete user");
    console.log(`User deleted:`, deleteUserResult.rows);

    /** Test 6: Edge case - Insert a user with missing fields */
    try {
      console.time("Insert user with missing fields");
      await client.query("INSERT INTO users (user_email) VALUES ($1)", ["missing.fields@example.com"]);
      console.timeEnd("Insert user with missing fields");
    } catch (error: unknown) {
      if (error instanceof Error) {
        console.log("Error inserting user with missing fields:", error.message);
      } else {
        console.log("Unknown error inserting user with missing fields:", error);
      }
    }

    /** Test 7: Boundary condition - Insert a user with a long name */
    const longName = "A".repeat(256); // Assuming the name column has a limitS
    try {
      console.time("Insert user with long name");
      await client.query("INSERT INTO users (user_email, user_password, user_name) VALUES ($1, $2, $3)", [
        "long.name@example.com",
        "password123",
        longName,
      ]);
      console.timeEnd("Insert user with long name");
    } catch (error: unknown) {
      if (error instanceof Error) {
        console.log("Error inserting user with long name:", error.message);
      } else {
        console.log("Unknown error inserting user with long name:", error);
      }
    }

    // Commit transaction
    await client.query("COMMIT");
    console.log("Query tests completed successfully!");
  } catch (error: unknown) {
    console.error("Error during query testing:", error);
    await client.query("ROLLBACK");
  } finally {
    client.release();
  }
}

// Run the query tests
testQueries().catch((error) => console.error("Error running query tests:", error));