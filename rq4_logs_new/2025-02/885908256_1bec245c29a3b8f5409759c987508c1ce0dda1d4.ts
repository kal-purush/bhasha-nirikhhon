interface ModelInletCost {
  [key: string]: number;
}

function parseInletCostConfig(config: string | undefined): ModelInletCost {
  if (!config) return {};

  // 如果配置是一个数字，对所有模型使用相同的预扣费
  const numericValue = Number(config);
  if (!isNaN(numericValue)) {
    return { default: numericValue };
  }

  // 否则解析 model1:0.32,model2:0.01 格式
  try {
    const costs: ModelInletCost = {};
    config.split(",").forEach((pair) => {
      const [model, cost] = pair.trim().split(":");
      const costValue = Number(cost);
      if (!isNaN(costValue)) {
        costs[model.trim()] = costValue;
      }
    });
    return costs;
  } catch (error) {
    console.error("Error parsing COST_ON_INLET config:", error);
    return {};
  }
}

export function getModelInletCost(modelId: string): number {
  const costConfig = parseInletCostConfig(process.env.COST_ON_INLET);
  return costConfig[modelId] ?? costConfig["default"] ?? 0;
}