import { Op } from "sequelize";
import { DataMeeting } from "../interfaces";
import { connectDB, Meeting } from "@/lib/db";

export const listMeetings = async (data: DataMeeting) => {
  const { who, when, whenEnd, since, until, about } = data;
  //console.log("DATA en listMeetings", data);
  await connectDB();
  const conditions: any = {};

  if (who && who.trim().length > 0) {
    conditions.who = {
      [Op.iLike]: `%${who}%`,
    };
  }

  if (when && whenEnd) {
    const startDate = when;
    const endDate = whenEnd;
    conditions.when = {
      [Op.gte]: startDate,
      [Op.lte]: endDate,
    };
  } else if (when) {
    const startDate = when;
    conditions.when = startDate;
  } else if (whenEnd) {
    const endDate = whenEnd;
    conditions.when = {
      [Op.lte]: endDate,
    };
  }

  if (since) {
    const startTime = since;
    conditions.since = {
      [Op.gte]: startTime,
    };
  }

  if (until) {
    const endTime = until;
    conditions.until = {
      [Op.lte]: endTime,
    };
  }

  if (about && about.trim().length > 0) {
    conditions.about = {
      [Op.iLike]: `%${about}%`,
    };
  }

  try {
    const meetings = await Meeting.findAll({
      where: conditions,
    });

    //console.log("Metings----------------------------->", meetings);
    return meetings;
  } catch (error) {
    console.error("Error fetching meetings:", error);
    throw error;
  }
};