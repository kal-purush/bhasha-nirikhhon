"use server";
import { setAuthtoken } from "./actions";
import { revalidatePath } from "next/cache";
import { MemberServices } from "@/services/member.services";
import { IAddMember } from "@/interfaces";

export async function getFreeMembers() {
  await setAuthtoken();
  const members = await MemberServices.getFree();
  revalidatePath("/dashboard");
  return members;
}
export async function addMembertoCompany(data: IAddMember) {
  await setAuthtoken();
  const res = await MemberServices.addToCompany(data);
  revalidatePath("/dashboard");
  return res;
}