import { atomWithStorage } from 'jotai/utils'

import type { History } from '@/types/history'

const historiesAtom = atomWithStorage<History[]>('histories', [])

export default historiesAtom