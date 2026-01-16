export interface Attribute {
  value: number,
  bonus: number,
}
export interface HighestAttribute extends Attribute {
  name: AttributeName,
}
export type Attributes = {
  [key in AttributeName]: Attribute
}

export type AttributeName =
  "Agility"    | "Fortitude"  | "Might"      | "Learning"  |
  "Logic"      | "Perception" | "Will"       | "Deception" |
  "Persuasion" | "Presence"   | "Alteration" | "Creation"  |
  "Energy"     | "Entropy"    | "Influence"  | "Movement"  |
  "Prescience" | "Protection"

export const AttributeNames: AttributeName[] = [
  "Agility"    , "Fortitude"  , "Might"      , "Learning"  ,
  "Logic"      , "Perception" , "Will"       , "Deception" ,
  "Persuasion" , "Presence"   , "Alteration" , "Creation"  ,
  "Energy"     , "Entropy"    , "Influence"  , "Movement"  ,
  "Prescience" , "Protection" ,
]