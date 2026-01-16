import type { CubeMember } from "@/types/cube";

const membersToCubeQuery = (members: Record<string, CubeMember[]>) => {
  return {
    measures: members.measures.map((m) => m.name),
    dimensions: members.dimensions.map((d) => d.name),
    segments: members.segments.map((s) => s.name),
    timeDimensions: members.timeDimensions.map((td) => ({
      dimension: td.name,
      granularity: td.granularity,
    })),
  };
};

export default membersToCubeQuery;