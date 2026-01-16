'use client'

import { useState, useEffect } from 'react'
import { useToast } from "@/hooks/use-toast"
import type { DeliberationProposal } from '@/types/deliberation'

interface DeliberationPhaseResponse {
  proposals: DeliberationProposal[];
  pendingCount: number;
  totalCount: number;
}

interface UseDeliberationPhaseResult {
  proposals: DeliberationProposal[];
  loading: boolean;
  setProposals: React.Dispatch<React.SetStateAction<DeliberationProposal[]>>;
  pendingCount: number;
  totalCount: number;
}

export function useDeliberationPhase(fundingRoundId: string): UseDeliberationPhaseResult {
  const [proposals, setProposals] = useState<DeliberationProposal[]>([])
  const [pendingCount, setPendingCount] = useState(0)
  const [totalCount, setTotalCount] = useState(0)
  const [loading, setLoading] = useState(true)
  const { toast } = useToast()

  useEffect(() => {
    async function fetchProposals() {
      try {
        const response = await fetch(
          `/api/funding-rounds/${fundingRoundId}/deliberation-proposals`
        )

        if (!response.ok) {
          throw new Error('Failed to fetch proposals')
        }

        const data: DeliberationPhaseResponse = await response.json()
        setProposals(data.proposals)
        setPendingCount(data.pendingCount)
        setTotalCount(data.totalCount)
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Failed to fetch proposals'
        toast({
          title: "Error",
          description: message,
          variant: "destructive",
        })
      } finally {
        setLoading(false)
      }
    }

    fetchProposals()
  }, [fundingRoundId, toast])

  return {
    proposals,
    loading,
    setProposals,
    pendingCount,
    totalCount,
  }
} 