import { IConfigCorpora } from '@/interfaces'
import { useMemo } from 'react'

const useCorpus = (
  corpora?: IConfigCorpora[],
  corpus_id?: string,
  watchCorpusValue?: string,
) => {
  return useMemo(() => {
    const getCorpusFromId = (corpusId?: string) => {
      const corp = corpora?.find(
        (corpus) => corpus.corpus_import_id === corpusId,
      )
      return corp ? corp : null
    }

    return getCorpusFromId(corpus_id ? corpus_id : watchCorpusValue)
  }, [corpora, corpus_id, watchCorpusValue])
}

export default useCorpus