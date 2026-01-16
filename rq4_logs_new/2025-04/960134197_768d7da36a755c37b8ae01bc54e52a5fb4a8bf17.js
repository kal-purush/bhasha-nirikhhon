"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const resume_controller_1 = require("./resume.controller");
describe('ResumeController', () => {
    let controller;
    let mockRepo;
    let mockRequest;
    let mockResponse;
    let responseJson;
    let responseStatus;
    beforeEach(() => {
        // Мокируем репозиторий
        mockRepo = {
            getAll: jest.fn(),
            getById: jest.fn(),
            create: jest.fn(),
            delete: jest.fn(),
            update: jest.fn(),
            getByUserId: jest.fn(),
        };
        // Инициализируем контроллер
        controller = new resume_controller_1.ResumeController(mockRepo);
        // Мокируем Express Request и Response
        mockRequest = {};
        responseJson = jest.fn();
        responseStatus = jest.fn(() => ({ json: responseJson }));
        mockResponse = {
            status: responseStatus,
            json: responseJson,
        };
    });
    describe('getResumes', () => {
        it('should return 200 and resumes list', () => __awaiter(void 0, void 0, void 0, function* () {
            const mockResumes = [
                { id: 1, title: 'Resume 1' },
                { id: 2, title: 'Resume 2' },
            ];
            // Настраиваем мок репозитория
            mockRepo.getAll.mockResolvedValue(mockResumes);
            // Вызываем метод контроллера
            yield controller.getResumes(mockRequest, mockResponse);
            // Проверяем, что репозиторий вызывался
            expect(mockRepo.getAll).toHaveBeenCalled();
            // Проверяем статус и ответ
            expect(mockResponse.status).toHaveBeenCalledWith(200);
            expect(mockResponse.json).toHaveBeenCalledWith(mockResumes);
        }));
        it('should return 500 if repository throws an error', () => __awaiter(void 0, void 0, void 0, function* () {
            // Мокируем ошибку
            mockRepo.getAll.mockRejectedValue(new Error('Database error'));
            yield controller.getResumes(mockRequest, mockResponse);
            expect(mockResponse.status).toHaveBeenCalledWith(500);
            expect(mockResponse.json).toHaveBeenCalledWith({ error: 'Database error' });
        }));
    });
    // Аналогичные тесты для других методов:
    // createResume, getCurrentResumes, getOneAdvert, deleteAdvert, updateAdvert
});