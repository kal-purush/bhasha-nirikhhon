import { useCallback, useEffect, useState } from "react";
import { getNfts } from "services/nfts";

import type { NFT } from "types/nft";

export function useNFT () {
  const [data, setData] = useState<NFT[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<Error | null>(null)

  const _getNft = useCallback(async () => {
    try {
      setIsLoading(true)

      const { data } = await getNfts()

      setData(data || [])

    } catch (err) {
      setError(new Error(err as any))
    } finally {
      setIsLoading(false)
    }
  }
, [])

  useEffect(() => {
    _getNft()
  }, [_getNft])

  return [
    data,
    isLoading,
    error
  ]
}