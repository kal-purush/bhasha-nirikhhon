export type MokletorgRoles =
  | "Manager"
  | "Maintainer"
  | "Graphic & UI/UX Designer"
  | "Committer"
  | "Contributor";

export interface Developer {
  name: string;
  role: string;
  picture: string;
  instagram: string;
  linkedin: string;
  website: string;
}

export const getRolePriority = (role: MokletorgRoles): number => {
  switch (role) {
    case "Manager":
      return 5;
    case "Maintainer":
      return 4;
    case "Graphic & UI/UX Designer":
      return 3;
    case "Committer":
      return 2;
    case "Contributor":
      return 1;
    default:
      return 0;
  }
};

export const getGenerationPriority = (role: string): number => {
  const match = role.match(/\((\d+)(st|nd|rd|th) Gen\.\)/);
  if (match) {
    const gen = parseInt(match[1]);
    return gen;
  }
  return 0;
};

export const sortData = (data: Developer[]): Developer[] => {
  return data.sort((a, b) => {
    const regex = /^(.+) \((.+)\)$/;

    const roleA = a.role.match(regex)?.[1] as MokletorgRoles;
    const roleB = b.role.match(regex)?.[1] as MokletorgRoles;

    const rolePriorityA = getRolePriority(roleA);
    const rolePriorityB = getRolePriority(roleB);

    if (rolePriorityA !== rolePriorityB) {
      return rolePriorityB - rolePriorityA; // sort by role
    } else {
      const generationPriorityA = getGenerationPriority(a.role);
      const generationPriorityB = getGenerationPriority(b.role);

      return generationPriorityB - generationPriorityA; // sort by gen
    }
  });
};