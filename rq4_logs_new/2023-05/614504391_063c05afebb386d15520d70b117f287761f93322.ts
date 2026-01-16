import { NextApiRequest, NextApiResponse } from "next";
import { getServerSession } from "next-auth/next";
import prisma from "@/lib/prisma";
import { authOptions } from "../auth/[...nextauth]";
import { Role, Status } from "@prisma/client";

interface JSONClause {
  [key: string]: any;
}

//here we handle all visits-related api calls
export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const session = await getServerSession(req, res, authOptions); //authenticate user on the server side
  // console.log(session.user?.role)
  if (!session)
    return res
      .status(401)
      .json({ success: false, message: "Unauthorized because not logged in" });


  //if method is PUT, check if the user is a doctor, patient or receptionist
  //changing date needs to be accomodated
  // Technnician can change status and technician note (Lab_assistant) he should also change results (Not yet in the db)
  // Supervisor can change status and supervisor notes (Lab_supervisor)
  
  if (req.method === "PUT") {
    var { test_id } = req.query;
    test_id = test_id.toString(); 
    const { role, id: user_id } = session.user;
    if (
      role === Role.DOCTOR ||
      role === Role.PATIENT
    ) {
      //if it was a doctor or patient, check that the visit belongs to them
      try {

        const exam = await prisma.laboratoryExamination.findUnique({
          where: {
           testId : test_id,
          },
          include: {
            visit: true,
          },
        });
        if (
          exam?.visit.doctorId !== user_id &&
          exam?.visit.patientId !== user_id
        ) {
          return res
            .status(401)
            .json({
              success: false,
              message: "Unauthorized, because this visit doesn't belong to you",
            });
        } else {
          try {  
            //
            const { status } = req.body;
            let data: JSONClause = {};
            if (status) {
              data.status = status;
            }
            await prisma.laboratoryExamination.update({
              where: {
                 testId: test_id,
               },
               data: data,
            });
          } catch (error) {
            return res
              .status(500)
              .json({ success: false, message: "failed to update visit" });
          }
          return res
            .status(200)
            .json({ success: true, message: "Visit updated successfully" });
        }
      } catch (error) {
        return res
          .status(404)
          .json({ success: false, message: "Failed to find the test" });
      }
    } else {
      return res
        .status(401)
        .json({
          success: false,
          message:
            "Unauthorized because you are not the correct person.. go away",
        });
    }
  }

 //Rols: All except registrar
 //Only patient is restricted to his own visits
 else if (req.method === "GET") {
    try {
      const { test_id } = req.query;
      let results: string | any;
      if (session.user?.role == Role.DOCTOR || session.user?.role == Role.PATIENT) {
        let person_id = session.user?.id;

        results = await prisma.laboratoryExamination.findUnique({
          where: {
            testId: test_id.toString()
          },
          include: {
            visit: true,
          },
        });

        if (results.visit.patientId === person_id || session.user?.role == Role.DOCTOR) {
          return res.status(200).json({ success: true, data: results });
        } else {
          return res
            .status(500)
            .json({ success: false, message: "The visit of the test does not belong to you" });
        }

      } 
      else {
        return res
          .status(500)
          .json({ success: false, message: "You should be a patient or a doctor to view this page" });
      }
    } catch (error) {
      //here should be a redirect to a general purpose error page
      return res
        .status(500)
        .json({ success: false, message: "Failed to retrieve Pysical examination" });
    }
  } else {
    return res
      .status(400)
      .json({ success: false, message: "Invalid request method" });
  }
}