
import { Log } from './index';

describe('qc-log_api', () => {

  describe('`Log`', () => {

    it('should be a function', () => {
      expect(typeof Log).toBe('function');
    });

    describe('`.Factory`', () => {

      it('should be an object', () => {
        expect(typeof Log.Factory).toBe('object');
      });

      describe('`.get`', () => {

        it('should be a function', () => {
          expect(typeof Log.Factory.get).toBe('function');
        });

        it('called with `undefined` should help code coverage', () => {
          Log.Factory.get();
        });

        it('called with `""` should return a Log instance', () => {
          let log: Log;

          log = Log.Factory.get("");
          expect(log).toBeInstanceOf(Log);
          expect(log.name).toBe('');
        });

        it('called with non-string should throw a `TypeError`', () => {
          expect(() => {
            Log.Factory.get(null);
          }).toThrow(TypeError);
        });

        it('called with `"."` should throw a `TypeError`', () => {
          expect(() => {
            Log.Factory.get(".");
          }).toThrow(TypeError);
        });

        it('called with `".foo"` should throw a `TypeError`', () => {
          expect(() => {
            Log.Factory.get(".foo");
          }).toThrow(TypeError);
        });

        it('called with `"foo."` should throw a `TypeError`', () => {
          expect(() => {
            Log.Factory.get("foo.");
          }).toThrow(TypeError);
        });

      });

    });

    describe('`.Level`', () => {

      it('should be an object', () => {
        expect(typeof Log.Level).toBe('object');
      });

      describe('`.TRACE`', () => {

        it('should be a number', () => {
          expect(typeof Log.Level.TRACE).toBe('number');
        });

      });

      describe('`.DEBUG`', () => {

        it('should be a number', () => {
          expect(typeof Log.Level.DEBUG).toBe('number');
        });

      });

      describe('`.INFO`', () => {

        it('should be a number', () => {
          expect(typeof Log.Level.INFO).toBe('number');
        });

      });

      describe('`.WARN`', () => {

        it('should be a number', () => {
          expect(typeof Log.Level.WARN).toBe('number');
        });

      });

      describe('`.ERROR`', () => {

        it('should be a number', () => {
          expect(typeof Log.Level.ERROR).toBe('number');
        });

      });

      describe('`.FATAL`', () => {

        it('should be a number', () => {
          expect(typeof Log.Level.FATAL).toBe('number');
        });

      });

    });

    describe('`.ROOT`', () => {

      it('should be an instance of `Log` with empty string for its name', () => {
        expect(Log.ROOT).toBeInstanceOf(Log);
        expect(Log.ROOT.name).toBe('');
      });

    });

    describe('`instances`', () => {

      it('should have a `trace` function', () => {
        expect(typeof Log.ROOT.trace).toBe('function');
        Log.ROOT.trace('testing %s', 'Log#trace');
      });

      it('should have a `debug` function', () => {
        expect(typeof Log.ROOT.debug).toBe('function');
        Log.ROOT.debug('testing %s', 'Log#debug');
      });

      it('should have a `error` function', () => {
        expect(typeof Log.ROOT.error).toBe('function');
        Log.ROOT.error('testing %s', 'Log#error');
      });

      it('should have a `fatal` function', () => {
        expect(typeof Log.ROOT.fatal).toBe('function');
        Log.ROOT.fatal('testing %s', 'Log#fatal');
      });

      it('should have a `info` function', () => {
        expect(typeof Log.ROOT.info).toBe('function');
        Log.ROOT.info('testing %s', 'Log#info');
      });

      it('should have a `warn` function', () => {
        expect(typeof Log.ROOT.warn).toBe('function');
        Log.ROOT.warn('testing %s', 'Log#warn');
      });

      it('should have a `logAt` function', () => {
        expect(typeof Log.ROOT.logAt).toBe('function');
        Log.ROOT.logAt(Log.Level.TRACE, 'testing %s with %s', 'Log#logAt', 'Log.Level.TRACE');
        Log.ROOT.logAt(Log.Level.DEBUG, 'testing %s with %s', 'Log#logAt', 'Log.Level.DEBUG');
        Log.ROOT.logAt(Log.Level.INFO, 'testing %s with %s', 'Log#logAt', 'Log.Level.INFO');
        Log.ROOT.logAt(Log.Level.WARN, 'testing %s with %s', 'Log#logAt', 'Log.Level.WARN');
        Log.ROOT.logAt(Log.Level.ERROR, 'testing %s with %s', 'Log#logAt', 'Log.Level.ERROR');
        Log.ROOT.logAt(Log.Level.FATAL, 'testing %s with %s', 'Log#logAt', 'Log.Level.FATAL');
      });

    });

  });

});