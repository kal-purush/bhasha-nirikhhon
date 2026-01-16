import { pget, ppost } from "utils/request";

export function loadKnowledgeIntro(knowledgeId) {
  return pget(`/rise/plan/knowledge/load/${knowledgeId}`)
}

export function learnKnowledge(knowledgeId) {
  return ppost(`/rise/plan/knowledge/learn/${knowledgeId}`)
}