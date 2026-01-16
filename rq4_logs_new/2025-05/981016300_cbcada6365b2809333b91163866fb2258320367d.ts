import { NextRequest, NextResponse } from 'next/server';
import { cookies } from 'next/headers';

// Session cookie configuration
const WALLET_SESSION_COOKIE = 'wallet_session';
const SESSION_MAX_AGE = 7 * 24 * 60 * 60; // 7 days in seconds

interface WalletSession {
  walletAddress: string;
  chainId: number;
  connectedAt: number;
}

/**
 * POST /api/auth/wallet-session
 * Creates or updates a wallet session
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { walletAddress, chainId } = body;
    
    // Validate required parameters
    if (!walletAddress || !chainId) {
      return NextResponse.json(
        { error: 'Missing required parameters' },
        { status: 400 }
      );
    }
    
    // Create session data
    const session: WalletSession = {
      walletAddress,
      chainId,
      connectedAt: Date.now(),
    };
    
    // Set encrypted httpOnly cookie with session data
    cookies().set({
      name: WALLET_SESSION_COOKIE,
      value: JSON.stringify(session),
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'strict',
      maxAge: SESSION_MAX_AGE,
      path: '/',
    });
    
    return NextResponse.json({ success: true }, { status: 200 });
  } catch (error) {
    console.error('Error creating wallet session:', error);
    return NextResponse.json(
      { error: 'Failed to create wallet session' },
      { status: 500 }
    );
  }
}

/**
 * GET /api/auth/wallet-session
 * Retrieves the current wallet session
 */
export async function GET() {
  try {
    const sessionCookie = cookies().get(WALLET_SESSION_COOKIE);
    
    if (!sessionCookie?.value) {
      return NextResponse.json(
        { isConnected: false, walletAddress: null, chainId: null },
        { status: 200 }
      );
    }
    
    try {
      const session: WalletSession = JSON.parse(sessionCookie.value);
      
      return NextResponse.json({
        isConnected: true,
        walletAddress: session.walletAddress,
        chainId: session.chainId,
        connectedAt: session.connectedAt,
      }, { status: 200 });
    } catch (parseError) {
      // Invalid session format, clear it
      cookies().delete(WALLET_SESSION_COOKIE);
      
      return NextResponse.json(
        { isConnected: false, walletAddress: null, chainId: null },
        { status: 200 }
      );
    }
  } catch (error) {
    console.error('Error retrieving wallet session:', error);
    return NextResponse.json(
      { error: 'Failed to retrieve wallet session' },
      { status: 500 }
    );
  }
}

/**
 * DELETE /api/auth/wallet-session
 * Clears the wallet session
 */
export async function DELETE() {
  try {
    console.log('API: Deleting wallet session cookie');
    
    // Set the cookie with immediate expiry to effectively delete it
    cookies().set({
      name: WALLET_SESSION_COOKIE,
      value: '',
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'strict',
      maxAge: 0, // Immediate expiration
      path: '/',
      expires: new Date(0), // Forces immediate expiration
    });
    
    // Also try to explicitly delete it
    cookies().delete(WALLET_SESSION_COOKIE);
    
    console.log('API: Wallet session cookie deleted');
    
    return NextResponse.json(
      { success: true, message: 'Session cleared' },
      { status: 200 }
    );
  } catch (error) {
    console.error('Error clearing wallet session:', error);
    return NextResponse.json(
      { error: 'Failed to clear wallet session' },
      { status: 500 }
    );
  }
}