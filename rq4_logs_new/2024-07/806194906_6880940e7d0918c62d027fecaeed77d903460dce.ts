import { useRecoilState } from "recoil";
import { userAtom } from "../atoms";
import { useEffect, useState } from "react";
import { usePathname, useRouter } from "next/navigation";

const useRoleValidator = () => {
    const [user] = useRecoilState(userAtom);
    const path = usePathname();
    const router = useRouter();
    const [status, setStatus] = useState(true);

    useEffect(() => {
        const pathSegments = path.split("/");
        const page = pathSegments[pathSegments.length - 1];

        const isAdminPage = ["companies", "users"].includes(page);
        const isEmployerPage = [
            "inventory-items",
            "inventory-rooms",
            "task-management",
            "teams",
        ].includes(page);
        const isWorkerPage = ["tasks"].includes(page);
        const isWorkerOrEmployerPage = ["profile", "archive"].includes(page);

        const hasAccess = () => {
            if (isAdminPage && user.role !== "admin") return false;
            if (isEmployerPage && user.role !== "employer") return false;
            if (isWorkerPage && user.role !== "worker") return false;
            if (
                isWorkerOrEmployerPage &&
                !["worker", "employer"].includes(user.role)
            )
                return false;
            return true;
        };

        if (!hasAccess()) {
            setStatus(false);
            router.push("/dashboard?status=1");
        } else {
            setStatus(true);
        }
    }, [path, user.role, router]);

    return status;
};

export default useRoleValidator;