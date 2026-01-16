import { Test, TestingModule } from '@nestjs/testing';
import { ExecutionContext } from '@nestjs/common';
import { CustomThrottlerGuard } from '../throttler.guard';
import { ThrottlerModule } from '@nestjs/throttler';

// Clase de prueba que hereda del guardia para acceder a métodos protegidos
class TestCustomThrottlerGuard extends CustomThrottlerGuard {
  // Exponer el método protegido para pruebas
  public testGetTracker(req: Record<string, any>): Promise<string> {
    return this.getTracker(req);
  }
}

describe('CustomThrottlerGuard', () => {
  let guard: TestCustomThrottlerGuard;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      imports: [
        ThrottlerModule.forRoot([
          {
            name: 'default',
            ttl: 60000,
            limit: 10,
          },
        ]),
      ],
      providers: [
        TestCustomThrottlerGuard,
      ],
    }).compile();

    guard = module.get<TestCustomThrottlerGuard>(TestCustomThrottlerGuard);
  });

  it('should be defined', () => {
    expect(guard).toBeDefined();
  });

  describe('getTracker', () => {
    it('should return tracker with IP and path', async () => {
      const req = {
        ip: '192.168.1.1',
        url: '/auth/login',
      };

      const result = await guard.testGetTracker(req as any);
      expect(result).toBe('192.168.1.1-/auth/login');
    });

    it('should use x-forwarded-for header if IP is not available', async () => {
      const req = {
        ip: null,
        headers: {
          'x-forwarded-for': '10.0.0.1, 10.0.0.2',
        },
        url: '/api/users',
      };

      const result = await guard.testGetTracker(req as any);
      expect(result).toBe('10.0.0.1-/api/users');
    });

    it('should use "unknown" if both IP and x-forwarded-for are missing', async () => {
      const req = {
        ip: null,
        headers: {},
        url: '/api/users',
      };

      const result = await guard.testGetTracker(req as any);
      expect(result).toBe('unknown-/api/users');
    });
  });

  describe('canActivate', () => {
    it('should be accessible for testing', () => {
      expect(guard.canActivate).toBeDefined();
    });
  });
}); 