"use client";

import { AppRouterInstance } from "next/dist/shared/lib/app-router-context.shared-runtime";
import { FormValues } from "../validation/formSchema";

export const handleFormSubmit =(
    data:FormValues,
    router: AppRouterInstance,
    handleClose:() => void
) => {
    const query = new URLSearchParams({
        totalTuitionFeeMin: data.totalTuitionFeeValue[0].toString(),
        totalTuitionFeeMax: data.totalTuitionFeeValue[1].toString(),
        movingOutsideThePrefecture: data.movingOutsideThePrefecture,
        commutingStyle: data.commutingStyle,
        highSchool: data.highSchool,
        attendanceFrequency: data.attendanceFrequency.join(","),
      }).toString();

      router.push(`/search?${query}`);
      handleClose();
};