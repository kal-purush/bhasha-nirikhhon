import type { NextApiRequest, NextApiResponse } from "next";
import pool from "@/lib/db";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { patientId } = req.query;

  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  if (!patientId || Array.isArray(patientId)) {
    return res.status(400).json({ error: "Invalid patient ID" });
  }

  try {
    const query = `
      SELECT
        mr.id,
        mr.patient_id,
        mr.doctor_id,
        mr.record_date,
        mr.diagnosis,
        mr.prescription,
        mr.notes
      FROM medical_records mr
      JOIN doctors d ON mr.doctor_id = d.id
      JOIN patients p ON mr.patient_id = p.id
      JOIN users u ON d.user_id = u.id
      WHERE mr.patient_id = $1
      ORDER BY mr.record_date DESC;
    `;

    const { rows } = await pool.query(query, [patientId]);
    res.status(200).json(rows);
  } catch (error: any) {
    console.error("API ERROR /medical-records/[id]:", error);
    res.status(500).json({ error: "Internal server error", details: error.message });
  }
}