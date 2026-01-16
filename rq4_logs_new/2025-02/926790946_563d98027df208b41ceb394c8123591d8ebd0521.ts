import { LoginServiceError } from "@/lib/CustomErrors";
import { IHasher } from "@/lib/Hasher";
import { ITokenService } from "@/lib/TokenService";
import { IAuthRepository } from "@/repository/AuthRepository";
import { IUserRepository } from "@/repository/UserRepository";
import AuthService from "@/services/AuthService";

const mockUserRepository: jest.Mocked<IUserRepository> = {
  createUser: jest.fn(),
  findUserByEmail: jest.fn(),
  findUserById: jest.fn(),
  updatePassword: jest.fn(),
};

const mockAuthRepository: jest.Mocked<IAuthRepository> = {
  createSessionToken: jest.fn(),
  findToken: jest.fn(),
  deleteAllTokens: jest.fn(),
};

const mockHasher: jest.Mocked<IHasher> = {
  encrypt: jest.fn(),
  decrypt: jest.fn(),
};

const mockTokenService: jest.Mocked<ITokenService> = {
  generate: jest.fn(),
  verify: jest.fn(),
};

describe("src/service/AuthService.ts", () => {
  let authService: AuthService;

  beforeEach(() => {
    jest.clearAllMocks();
    authService = new AuthService(mockUserRepository, mockAuthRepository, mockHasher, mockTokenService);
  });

  describe("register method:", () => {
    describe("Successfull Cases", () => {
      test("Should create a new user successfully", async () => {
        const user = {
          name: "John Doe",
          email: "john@email.com",
          password: "P4ssword!23",
          passwordConfirmation: "P4ssword!23",
        };

        mockUserRepository.findUserByEmail.mockResolvedValue(null);
        mockHasher.encrypt.mockResolvedValue("hashed-password");
        mockUserRepository.createUser.mockResolvedValue(undefined);

        await expect(authService.register(user)).resolves.toBeUndefined();
      });
    });

    describe("Failure Cases", () => {
      test("Should throw an error if user exists", async () => {
        const existingUser = {
          id: "123",
          name: "John Doe",
          email: "john@email.com",
          password: "P4ssword!23",
          createdAt: new Date(Date.now()),
        };

        const newUser = {
          id: "123",
          name: "John Doe",
          email: "john@email.com",
          password: "P4ssword!23",
          passwordConfirmation: "P4ssword!23",
        };

        mockUserRepository.findUserByEmail.mockResolvedValue(existingUser);

        await expect(authService.register(newUser)).rejects.toThrow("Credenciais inválidas");
      });
    });
  });

  describe("login method:", () => {
    describe("Successfull Cases", () => {
      test("Should return a session token when user succesfull logged in", async () => {
        const userData = {
          id: "123",
          name: "John Doe",
          email: "john@email.com",
          password: "P4ssword!23",
          createdAt: new Date(Date.now()),
        };

        const sessionToken = "fake-token";

        mockUserRepository.findUserByEmail.mockResolvedValue(userData);
        mockHasher.decrypt.mockResolvedValue(true);
        mockTokenService.generate.mockResolvedValue(sessionToken);

        const result = await authService.login(userData.email, userData.password);
        expect(result).toBe(sessionToken);
      });
    });
    describe("Failure Cases", () => {
      test("Should throw an error if user don't exist", async () => {
        mockUserRepository.findUserByEmail.mockResolvedValue(null);

        await expect(authService.login("unexistent@email.com", "unexistent")).rejects.toThrow(LoginServiceError);
      });

      test("Shold throw an error if password is incorrect", async () => {
        const userData = {
          id: "123",
          name: "John Doe",
          email: "john@email.com",
          password: "P4ssword!23",
          createdAt: new Date(Date.now()),
        };

        mockUserRepository.findUserByEmail.mockResolvedValue(userData);
        mockHasher.decrypt.mockResolvedValue(false);

        await expect(() => authService.login("john@email.com", "wrong-password")).rejects.toThrow(LoginServiceError);
      });
    });
  });

  describe("logout method", () => {
    describe("Successfull Cases", () => {});
    describe("Failure Cases", () => {});
  });
});