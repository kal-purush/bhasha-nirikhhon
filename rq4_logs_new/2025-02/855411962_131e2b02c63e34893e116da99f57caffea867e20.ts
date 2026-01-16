import type { Rpc } from '../Rpc/Rpc.ts'

export const create = ({ invoke }: { invoke: any }): Rpc => {
  const mockRpc = {
    invoke,
  } as any
  return mockRpc
}