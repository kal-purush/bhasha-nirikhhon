import { Connection } from "typeorm";
import { unsortedIndustries } from "../../../shared/industries";
import { Industry } from "../entity/Industry";
import { SubIndustry } from "../entity/SubIndustry";

export const seedIndustries = async (conn: Connection) => {
  for (let i of unsortedIndustries) {
    const newIndustry = await conn.manager
      .create(Industry, {
        code: i.code.toString(),
        nameEn: i.names.en,
        nameFi: i.names.fi,
        subIndustries: [],
      })
      .save();
    for (let s of i.subIndustries) {
      const newSubIndustry = await conn.manager
        .create(SubIndustry, {
          code: s.code.toString(),
          nameEn: s.names.en,
          nameFi: s.names.fi,
          industry: newIndustry,
        })
        .save();
      newIndustry.subIndustries =
        newIndustry.subIndustries.concat(newSubIndustry);
      await newIndustry.save();
    }
  }
  console.log("seeded industries");
};