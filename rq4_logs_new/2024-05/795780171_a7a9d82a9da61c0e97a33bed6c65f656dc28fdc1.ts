"use server";

import { RugbyPayment } from "@prisma/client";
import { resend } from "@/lib/resend";
import { getMemberById } from "./get-member-by-id";
import { EmailTemplate } from "@/components/email-template";

export const chargeMembers = async (members: RugbyPayment[]) => {
  members.map(async (member) => {
    const memberInfo = await getMemberById(member.memberId);

    console.log(memberInfo);

    if (memberInfo) {
      const response = await resend.emails.send({
        from: "financeiro@rugbymaua.com.br",
        to: memberInfo.email,
        subject: "Rugby Mauá Cobrança de Mensalidade",
        html: `<h1>Olá ${memberInfo.name}</h1>`,
      });

      console.log(response);
    }
  });
};