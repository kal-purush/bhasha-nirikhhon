import { Test } from '@nestjs/testing';
import { AsicsRepository } from '@modules/asics/asics.repository';
import { AsicsService } from '@modules/asics/asics.service';
import { AsicsRepositoryMock } from './mocks/asics.repository.mock';
import { AsicsAutomationService } from '@modules/asics/asics-automation.service';
import { AsicsScalingStrategyExecutor } from '@modules/asics/strategies';
import { ControlRuleService } from '@modules/automation/control-rule/control-rule.service';
import { Cache } from 'cache-manager';
import { CACHE_MANAGER } from '@nestjs/cache-manager';
import { AsicsServiceMock } from './mocks/asics.service.mock';
import { ControlRuleServiceMock } from '../automation/control-rule/mocks/control-rule.service.mock';
import { AsicsScalingStrategyExecutorMock } from './strategies/scaling/mocks/asics-scaling-strategy.executor.mock';
import { PROTECTION_RESULT_KEY } from '@modules/automation/protection-rule/protection-rule.constants';
import { LogType } from '@modules/logs/enums';
import { ControlRuleRepositoryMock } from '../automation/control-rule/mocks/control-rule.repository.mock';

describe('AsicsAutomationService', () => {
  let service: AsicsAutomationService;
  let cache: Cache;
  let asicsRepository: AsicsRepository;
  let asicsService: AsicsService;
  let controlRuleService: ControlRuleService;
  let asicsScalingStrategyExecutor: AsicsScalingStrategyExecutor;

  const { asicsMock } = AsicsRepositoryMock;
  const { controlRuleMock, controlRulesMock } = ControlRuleRepositoryMock;

  beforeEach(async () => {
    const module = await Test.createTestingModule({
      providers: [
        AsicsAutomationService,
        {
          provide: CACHE_MANAGER,
          useClass: Map,
        },
        {
          provide: AsicsRepository,
          useClass: AsicsRepositoryMock,
        },
        {
          provide: AsicsService,
          useClass: AsicsServiceMock,
        },
        {
          provide: ControlRuleService,
          useClass: ControlRuleServiceMock,
        },
        {
          provide: AsicsScalingStrategyExecutor,
          useClass: AsicsScalingStrategyExecutorMock,
        },
      ],
    }).compile();

    service = module.get(AsicsAutomationService);
    cache = module.get(CACHE_MANAGER);
    asicsRepository = module.get(AsicsRepository);
    asicsService = module.get(AsicsService);
    controlRuleService = module.get(ControlRuleService);
    asicsScalingStrategyExecutor = module.get(AsicsScalingStrategyExecutor);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('handleStartAsicsCron', () => {
    let findWhereSpy: jest.SpiedFunction<AsicsRepository['findWhere']>;
    let startAsicsSpy: jest.SpiedFunction<AsicsService['startAsics']>;

    beforeEach(() => {
      findWhereSpy = jest.spyOn(asicsRepository, 'findWhere');
      startAsicsSpy = jest.spyOn(asicsService, 'startAsics');
    });

    it('should not start asics when protection is triggered', async () => {
      await cache.set(PROTECTION_RESULT_KEY, { triggered: true });

      await service.handleStartAsicsCron();

      expect(findWhereSpy).not.toHaveBeenCalled();
      expect(startAsicsSpy).not.toHaveBeenCalled();
    });

    it('should start asics when protection is not triggered', async () => {
      await service.handleStartAsicsCron();

      expect(findWhereSpy).toHaveBeenCalledWith({ t2Active: true });
      expect(startAsicsSpy).toHaveBeenCalledWith(asicsMock, LogType.CONTROL);
    });
  });

  describe('handleStopAsicsCron', () => {
    let findWhereSpy: jest.SpiedFunction<AsicsRepository['findWhere']>;
    let stopAsicsSpy: jest.SpiedFunction<AsicsService['stopAsics']>;

    beforeEach(() => {
      findWhereSpy = jest.spyOn(asicsRepository, 'findWhere');
      stopAsicsSpy = jest.spyOn(asicsService, 'stopAsics');
    });

    it('should not stop asics when protection is triggered', async () => {
      await cache.set(PROTECTION_RESULT_KEY, { triggered: true });

      await service.handleStopAsicsCron();

      expect(findWhereSpy).not.toHaveBeenCalled();
      expect(stopAsicsSpy).not.toHaveBeenCalled();
    });

    it('should start asics when protection is not triggered', async () => {
      await service.handleStopAsicsCron();

      expect(findWhereSpy).toHaveBeenCalledWith({ t2EndStop: true });
      expect(stopAsicsSpy).toHaveBeenCalledWith(asicsMock, LogType.CONTROL);
    });
  });

  describe('restartAsicScalingStrategies', () => {
    it('should trigger asics scaling executor when control rule updated', () => {
      const executeSpy = jest.spyOn(asicsScalingStrategyExecutor, 'execute');

      service.restartAsicScalingStrategies(controlRuleMock);

      expect(executeSpy).toHaveBeenCalledWith(controlRuleMock);
    });
  });

  describe('onApplicationBootstrap', () => {
    it('should trigger asics scaling executor when control rule updated', async () => {
      const getRulesSpy = jest.spyOn(controlRuleService, 'getRules');
      const executeSpy = jest.spyOn(asicsScalingStrategyExecutor, 'execute');

      await service.onApplicationBootstrap();

      expect(getRulesSpy).toHaveBeenCalled();

      expect(executeSpy).toHaveBeenCalledWith(controlRulesMock);
    });
  });
});