import { userDetails } from "@/types/cs2";

export async function fetchEloDetails(rank: string): Promise<userDetails | null> {
    const res = await fetch(`/api/proxy/estimate/cs2/${rank}`);
    if (!res.ok) {
        console.log(res);
        return null;
    }

    const data: userDetails = await res.json();
    return data;
}