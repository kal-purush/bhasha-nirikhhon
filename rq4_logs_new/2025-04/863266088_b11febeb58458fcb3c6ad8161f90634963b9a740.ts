import { useEffect, useState } from "react";
import useCustomAccount from "./use-account";
import { get } from "@/utils/http";

export default function useUserPoints() {
  const { account } = useCustomAccount()
  const [loading, setLoading] = useState(false)
  const [userPoints, setUserPoints] = useState()
  async function getUserPoints() {
    const result = await get("https://dev-api.beratown.app/infrared?path=api%2Fpoints%2Fuser%2F" + account + "&params=chainId%3D80094")
    console.log('====result', result)
    setUserPoints(result)
  }
  useEffect(() => {
    account && getUserPoints()
  }, [account])

  return {
    loading,
    userPoints
  }
}