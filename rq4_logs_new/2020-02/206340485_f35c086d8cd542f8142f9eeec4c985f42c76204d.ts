import {hash, is, ValueObject, Set} from "immutable"
import {Slot} from "./types"
import {Skill} from "./Skill"
import {LeveledSkill} from "./LeveledSkill"

export class Decoration implements ValueObject {
  static of = (size: number, skill: string) => new Decoration(size, [new LeveledSkill(1, new Skill(skill))])

  readonly leveledSkillsSet: Set<LeveledSkill>
  constructor(readonly size: Slot, readonly leveledSkills: LeveledSkill[]) {
    this.leveledSkillsSet = Set(leveledSkills)
  }

  hasSkill(skill: Skill) {
    return this.leveledSkills.some(leveledSkill => leveledSkill.skill.equals(skill))
  }

  hashCode = () => hash(this.leveledSkillsSet)
  equals = (other: Decoration) => is(this.size, other.size) && is(this.leveledSkillsSet, other.leveledSkillsSet)
}