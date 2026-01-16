/// <reference path="test.ts" />

module Report {
    class Reporter {
        tests: Test.IContainer;
        constructor(tests: Test.IContainer) {
            this.tests = tests;
        }
    }

    export class Html extends Reporter {
        lookup: any;
        passed: boolean;

        constructor(tests: Test.IContainer)
        {
            super(tests);
            this.lookup = {};
            this.lookup[Test.State.pass] = 'pass';
            this.lookup[Test.State.fail] = 'fail';
            this.lookup[Test.State.skip] = 'skip';
            this.lookup[Test.State.none] = 'none';
        }

        run(element?: HTMLElement) {
            if (!element) {
                element = document.body;
            }

            this.passed = this.tests.run();
            this.output(element);
        }

        output(element: HTMLElement) {
            var results = this.tests.results(),
            html = '<div class="status ' + (this.passed ? 'pass' : 'fail') +'">' + this.status(results) + '</div>';
            html += '<ol class="results">';
            results.forEach(result => html += this.line(result));
            html += '</ol>';
            element.innerHTML = html;
        }

        private line(result: Test.IResult): string {
            return '<li class="' + this.lookup[result.state] + '">' + result.path + (result.message ? '<pre>' + result.message + '</pre>' : '') + '</li>';
        }

        private status(results: Test.IResult[]) {
            var passed = results.filter(item=> item.state == Test.State.pass).length,
                failed = results.filter(item=> item.state == Test.State.fail).length,
                skipped = results.filter(item=> item.state == Test.State.skip).length;
            return (passed ? passed + ' test' + (passed == 1 ? '' : 's') + ' passed. ' : '')
                 + (failed ? failed + ' test' + (failed == 1 ? '' : 's') + ' failed. ' : '')
                 + (skipped ? skipped + ' test' + (skipped == 1 ? '' : 's') + ' skipped. ' : '')
        }
    }
}