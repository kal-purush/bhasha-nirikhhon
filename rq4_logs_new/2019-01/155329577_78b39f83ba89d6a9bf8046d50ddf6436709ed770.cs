// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Agent.Plugins.Log.TestResultParser.Parser
{
    using System;
    using System.Collections.Generic;
    using System.Text.RegularExpressions;
    using Agent.Plugins.Log.TestResultParser.Contracts;

    /// <summary>
    /// Base class for all node parsers
    /// </summary>
    public abstract class NodeParserStateBase : ITestResultParserState
    {
        protected string parserName;
        protected string stateName;
        protected ITraceLogger logger;
        protected ITelemetryDataCollector telemetryDataCollector;
        protected ParserResetAndAttemptPublish attemptPublishAndResetParser;

        /// <summary>
        /// List of Regexs and their corresponding post successful match actions
        /// </summary>
        public virtual IEnumerable<RegexActionPair> RegexesToMatch => throw new NotImplementedException();

        /// <summary>
        /// Constructor for a node parser state
        /// </summary>
        /// <param name="parserResetAndAttempPublish">Delegate sent by the parser to reset the parser and attempt publication of test results</param>
        /// <param name="logger"></param>
        /// <param name="telemetryDataCollector"></param>
        protected NodeParserStateBase(ParserResetAndAttemptPublish parserResetAndAttempPublish, ITraceLogger logger, ITelemetryDataCollector telemetryDataCollector, string parserName)
        {
            this.logger = logger;
            this.parserName = parserName;
            this.stateName = this.GetType().Name;
            this.telemetryDataCollector = telemetryDataCollector;
            this.attemptPublishAndResetParser = parserResetAndAttempPublish;
        }

        /// <summary>
        /// Default implemenation that checks for the constraint for the next expected match
        /// If the number of lines within which the next match expected falls to 0 this resets the parser
        /// </summary>
        /// <param name="line">Current line</param>
        /// <param name="stateContext">State context object containing information of the parser's state</param>
        /// <returns>True if the parser was reset</returns>>
        public virtual bool PeformNoPatternMatchedAction(string line, AbstractParserStateContext stateContext)
        {
            // This is a mechanism to enforce matches that have to occur within 
            // a specific number of lines after encountering the previous match
            // one obvious usage is for successive summary lines containing passed,
            // pending and failed test summary
            if (stateContext.LinesWithinWhichMatchIsExpected == 1)
            {
                this.logger.Info($"{this.parserName} : {this.stateName} : NoPatternMatched : Was expecting {stateContext.NextExpectedMatch} before line {stateContext.CurrentLineNumber}, but no matches occurred.");
                this.attemptPublishAndResetParser();
                return true;
            }

            // If no match occurred and a match was expected in a positive number of lines, decrement the counter
            // A value of zero or lesser indicates not expecting a match
            if (stateContext.LinesWithinWhichMatchIsExpected > 1)
            {
                stateContext.LinesWithinWhichMatchIsExpected--;
            }

            return false;
        }

        /// <summary>
        /// Returns a test result with the outcome set and name extracted from the match
        /// </summary>
        /// <param name="testOutcome">Outcome of the test</param>
        /// <param name="match">Match object for the test case result</param>
        /// <returns></returns>
        protected TestResult PrepareTestResult(TestOutcome testOutcome, Match match)
        {
            return new TestResult
            {
                Outcome = testOutcome,
                Name = match.Groups[RegexCaptureGroups.TestCaseName].Value
            };
        }
    }
}