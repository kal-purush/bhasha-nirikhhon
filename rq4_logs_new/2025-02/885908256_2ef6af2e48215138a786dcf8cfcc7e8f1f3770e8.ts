import { NextResponse } from "next/server";

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

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const modelId = searchParams.get("model");

    if (!modelId) {
      return NextResponse.json(
        { error: "Model ID is required" },
        { status: 400 }
      );
    }

    const costConfig = parseInletCostConfig(process.env.COST_ON_INLET);

    // 获取特定模型的预扣费，如果没有则使用默认值，如果也没有默认值则返回0
    const inletCost = costConfig[modelId] ?? costConfig["default"] ?? 0;

    return NextResponse.json({
      success: true,
      cost: inletCost,
    });
  } catch (error) {
    console.error("Error getting inlet cost:", error);
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Unknown error",
      },
      { status: 500 }
    );
  }
}