import { NextResponse } from 'next/server';
import { calculateSMA, calculateEMA, calculateRSI, calculateMACD } from '../../../../lib/technicalIndicators';

export async function GET(
  request: Request,
  { params }: { params: { symbol: string } }
) {
  const { symbol } = params;

  try {
    // Fetch historical price data (this would come from your crypto service)
    const priceData = await getHistoricalPrices(symbol);
    
    // Calculate indicators
    const sma = calculateSMA(priceData, 14);
    const ema = calculateEMA(priceData, 14);
    const rsi = calculateRSI(priceData, 14);
    const macd = calculateMACD(priceData);

    return NextResponse.json({
      sma,
      ema,
      rsi,
      macd
    });
  } catch (error) {
    return NextResponse.json(
      { error: 'Failed to calculate indicators' },
      { status: 500 }
    );
  }
}

// Mock function - replace with actual price data fetching
async function getHistoricalPrices(symbol: string): Promise<number[]> {
  // Mock data - replace with actual price data fetching
  return [
    40000,
    40500,
    41000,
    41500,
    42000,
    42500,
    43000,
    43500,
    44000,
    44500,
    45000,
    45500,
    46000,
    46500
  ];
}