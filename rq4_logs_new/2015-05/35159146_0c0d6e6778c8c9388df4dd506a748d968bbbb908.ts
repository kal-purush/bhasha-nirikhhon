/// <reference path="test.ts" />

module Report {
    class Reporter {
        tests: Test.IContainer;
        lookup: any;
        duration: number;
        passed: boolean;
        include: Test.State[];
        results: Test.IResult[];
        filteredresults: Test.IResult[];

        constructor(tests: Test.IContainer) {
            this.tests = tests;
            this.include = [Test.State.none, Test.State.pass, Test.State.fail, Test.State.skip];
            this.lookup = {};
            this.lookup[Test.State.pass] = 'pass';
            this.lookup[Test.State.fail] = 'fail';
            this.lookup[Test.State.skip] = 'skip';
            this.lookup[Test.State.none] = 'none';
        }

        run(): any {
            var start = performance.now();
            this.passed = this.tests.run();
            this.duration = performance.now() - start;
            this.results = this.tests.results();
            this.filteredresults = this.results.filter(item => this.include.some(value => value == item.state));
            return this.output();
        }

        output(): any { throw Error; }

        protected timer() {
            return 'Tests completed in ' + this.duration.toFixed(2) + ' milliseconds.';
        }

        protected status() {
            var passed = this.results.filter(item => item.state == Test.State.pass).length,
                failed = this.results.filter(item => item.state == Test.State.fail).length,
                skipped = this.results.filter(item => item.state == Test.State.skip).length,
                discovered = this.results.length,
                messages = [];

            if (passed != discovered) {
                messages.push(discovered + ' test' + (discovered == 1 ? '' : 's') + ' discovered.');
            }

            if (passed) {
                messages.push(passed + ' test' + (passed == 1 ? '' : 's') + ' passed.');
            }

            if (failed) {
                messages.push(failed + ' test' + (failed == 1 ? '' : 's') + ' failed.');
            }

            if (skipped) {
                messages.push(skipped + ' test' + (skipped == 1 ? '' : 's') + ' skipped.');
            }

            return messages.join(' ');
        }
    }

    export class String extends Reporter {

        output(): string {
            var max: number = this.filteredresults.reduce(function (a, b) { return a.path.length > b.path.length ? a : b; }).path.length,
                out: string = 'info - ' + this.status() + '\n';
            out += 'info - ' + this.timer() + '\n';
            this.filteredresults.forEach(result => out += this.line(result, max));
            return out;
        }

        private line(result: Test.IResult, max: number): string {
            var path = result.path,
                message = [];

            while (path.length < max) {
                path += ' ';
            }

            message.push(this.lookup[result.state]);
            message.push(path);
            if (result.message) {
                message.push(result.message);
            }

            return message.join(' - ') + '\n';
        }
    }

    export class Console extends Reporter {
        color: any;

        constructor(tests: Test.IContainer) {
            super(tests);
            this.color = {};
            this.color[Test.State.pass] = '#9CCF31';
            this.color[Test.State.fail] = '#FF9E00';
            this.color[Test.State.skip] = '#009ECE';
            this.color[Test.State.none] = '#808080';
        }

        output() {
            var max: number = this.filteredresults.reduce(function (a, b) { return a.path.length > b.path.length ? a : b; }).path.length;
            console.log('%cinfo - ' + this.status(), 'color: ' + this.color[this.passed ? Test.State.pass : Test.State.fail]);
            console.log('%cinfo - ' + this.timer(), 'color: ' + this.color[this.passed ? Test.State.pass : Test.State.fail]);
            this.filteredresults.forEach(result => this.line(result, max));
        }

        private line(result: Test.IResult, max: number): void {
            var path = result.path,
                message = [];

            while (path.length < max) {
                path += ' ';
            }

            message.push(this.lookup[result.state]);
            message.push(path);
            if (result.message) {
                message.push(result.message);
            }

            console.log('%c' + message.join(' - '), 'color: ' + this.color[result.state]);
        }
    }

    export class Html extends Reporter {
        output() {
            var html = '<div class="status ' + this.lookup[this.passed ? Test.State.pass : Test.State.fail] + '">' + this.status() + '</div>';
            html += '<div class="timer ' + this.lookup[this.passed ? Test.State.pass : Test.State.fail] + '">' + this.timer() + '</div>';
            html += '<ol class="results">';
            this.filteredresults.forEach(result => html += this.line(result));
            html += '</ol>';
            return html;
        }

        private line(result: Test.IResult): string {
            return '<li class="' + this.lookup[result.state] + '">' + result.path + (result.message ? '<pre>' + result.message + '</pre>' : '') + '</li>';
        }
    }

    export class Email extends Reporter {
        color: any;
        statusStyle: string = 'position: relative; padding: 7px 8px 7px 30px; color: white; font-size: large; font-weight: bold;';
        timerStyle: string = 'position: relative; padding: 0 8px 7px 30px; color: white; font-size: large; font-weight: bold;';
        resultsStyle: string = 'color: white; padding: 0; margin: 0 auto;';
        resultStyle: string = 'position: relative; padding: 8px 8px 8px 30px; list-style: none;';
        preStyle: string = 'background: white; color: initial; padding: 5px; border-radius: 6px; margin: 10px 0 0 0;';

        constructor(tests: Test.IContainer) {
            super(tests);
            this.include = [Test.State.none, Test.State.fail ];
            this.color = {};
            this.color[Test.State.pass] = '#9CCF31';
            this.color[Test.State.fail] = '#FF9E00';
            this.color[Test.State.skip] = '#009ECE';
            this.color[Test.State.none] = '#808080';
        }

        output() {
            var html = '<div style="' + this.statusStyle + 'background-color:' + (this.color[this.passed ? Test.State.pass : Test.State.fail]) + '">' + this.status() + '</div>';
            html += '<div style="' + this.timerStyle + 'background-color:' + (this.color[this.passed ? Test.State.pass : Test.State.fail]) + '">' + this.timer() + '</div>';
            html += '<ol style="' + this.resultsStyle + '">';
            this.filteredresults.forEach(result => html += this.line(result));
            html += '</ol>';
            return html;
        }

        private line(result: Test.IResult): string {
            return '<li style="' + this.resultStyle + 'background-color:' + this.color[result.state] + '">' + result.path + (result.message ? '<pre style="' + this.preStyle + '">' + result.message + '</pre>' : '') + '</li>';
        }
    }
}