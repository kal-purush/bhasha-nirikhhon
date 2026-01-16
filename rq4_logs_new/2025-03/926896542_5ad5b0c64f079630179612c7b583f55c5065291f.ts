'use client'
import { useSupabaseSession } from "@/app/_hooks/useSupabaseSession";
import { useRouter } from "next/navigation";
import { useEffect } from "react";

const useRouteGuard = () => {
  const router = useRouter()
  const { session } = useSupabaseSession()

  useEffect(() => {
    if (session === undefined) return // sessionがundefinedの場合は読み込み中なので何もしない

    const fetcher = async () => {
      if (session === null) {
        console.log("🔄 未ログインのため /login にリダイレクトします");
        router.replace('/login')
      }
    }

    fetcher()
    
  },[router, session])
}

export default useRouteGuard;