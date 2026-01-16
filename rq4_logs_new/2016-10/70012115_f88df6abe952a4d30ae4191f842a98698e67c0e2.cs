using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TuringMachineApp
{
    class Flattener
    {
        static string SZMOVE_RIGHT = ">";
        static string SZMOVE_LEFT = "<";
        static string SZSTAY = "-";

        static string END_SYMBOL = "$";
        static string RETURN_SYMBOL = "R";
        static string NULL_SYMBOL = "~";

        readonly int NUM_TAPES;
        TuringMachine TM;

        class SampleCollection
        {   
            public string this[string i]
            {
                get { return i + "."; }
            }
        }

        SampleCollection SymMap = new SampleCollection();

        public Flattener(TuringMachine tf)
        {

            NUM_TAPES = tf.TransitionFunctions.First().DomainHeadValues.Count;
            TM = tf;
        }


        /// <summary>
        /// Builds up all the undetermined states beginning from the 'basestate'
        ///  based on the provided transition functions. This does not include
        ///  states that map uniquely to the domain of a TF.
        /// E.G.
        /// baseState := 'q1'
        /// TF := q1,_,1,_ -> xxx
        /// 
        /// Yields states q1_, and q1_1
        /// NOT q1_1_
        /// </summary>
        /// <param name="baseState">State that maps to equivalent mKState</param>
        /// <param name="determiningTFs">Transition Functions from the mKTFs that begin at 'baseState'</param>
        /// <returns></returns>
        private IEnumerable<State>
            E_BuildUnderterminedStates(
            State baseState,
            IEnumerable<TransitionFunction> determiningTFs)
        {
            foreach (TransitionFunction matchedTF in determiningTFs)
            {
                for (int i = 0; i < NUM_TAPES; i++)
                {
                    State permState;

                    if (i == 0)
                    {
                        permState = new State(baseState.Actual, baseState);
                    }
                    else
                    {
                        // If a transition function takes in (q1, 1, 0); then this will generate state q1io 
                        permState = new State(matchedTF.DomainState.Actual);
                        for (int k = 0; k < i; k++)
                        {
                            string headSymbol = SymMap[matchedTF.DomainHeadValues[k]];
                            permState = new State(
                               permState.Actual + headSymbol,
                               permState,
                               headSymbol);
                        }
                    }
                    yield return permState;
                }
            }
        }

        /// <summary>
        /// Builds a unique state that matches the parameters of all
        ///  transition functions that start from 'baseState'
        /// E.G.
        /// baseState := 'q1'
        /// TF := q1,_,1,_ -> xxx
        /// 
        /// Yields q1_1_
        /// </summary>
        /// <param name="baseState">State that maps to equivalent mKState</param>
        /// <param name="determiningTFs">Transition Functions from the mKTFs that begin at 'baseState'</param>
        /// <returns></returns>
        private IEnumerable<DeterminedState>
            E_BuildDeterminedStates(
            State baseState,
            IEnumerable<TransitionFunction> determiningTFs)
        {
            int i = NUM_TAPES;
            foreach (TransitionFunction matchedTF in determiningTFs)
            {
                State permState = new State(matchedTF.DomainState.Actual);
                for (int k = 0; k < i; k++)
                {
                    string headSymbol = SymMap[matchedTF.DomainHeadValues[k]];
                    permState = new State(
                       permState.Actual + headSymbol,
                       permState,
                       headSymbol);
                }
                DeterminedState returnState = new DeterminedState(
                    permState.Actual,
                    permState,
                    permState.Actual,
                    matchedTF);
                yield return returnState;
            }
        }

        private IEnumerable<DeterminedState>
            E_BuildDeterminedStates_ActorStates_N_Write(
            IEnumerable<DeterminedState> determinedStates)
        { 
            foreach (DeterminedState dstate in determinedStates)
            {
                // Since each dstate is unique, states and tfs can be added immediately.
                for (int i = 1; i <= NUM_TAPES; i++)
                {

                    DeterminedState dstateNthTape = new DeterminedState(
                        dstate.Actual + i,
                        dstate,
                        dstate.Actual,
                        dstate.TF,
                        (i).ToString());
                    yield return dstateNthTape;
                }
            }
        }

        private IEnumerable<DeterminedState>
            E_BuildDeterminedStates_ActorStates_0F_Complete(
            IEnumerable<DeterminedState> determinedStates)
        {
            foreach (DeterminedState dstate in determinedStates)
            {
                DeterminedState permStateCompleted = new DeterminedState(
                        dstate.Actual + "F",
                        dstate,
                        dstate.Actual,
                        dstate.TF,
                        "F");
                yield return permStateCompleted;
            }
        }

        private IEnumerable<DeterminedState>
            E_BuildDeterminedStates_ActorStates_N_MoveHead(
            IEnumerable<DeterminedState> determinedStates_ActorStates_Write)
        {

            foreach (DeterminedState dstateNthTape in determinedStates_ActorStates_Write)
            {
                DeterminedState dstateNthTape_MoveHead = new DeterminedState(
                        dstateNthTape.Actual + "a",
                        dstateNthTape,
                        dstateNthTape.BaseState,
                        dstateNthTape.TF,
                        ("a").ToString());
                yield return dstateNthTape_MoveHead;
            }
        }


        public IEnumerable<TransitionFunction>
        Sweep_Right_XthParm_MoveToNext(
                    IEnumerable<State> undeterminedStates,
                    List<string> NonHeaderLibrary)
        {
            foreach (State permState in undeterminedStates)
            {
                foreach (string nonheader in NonHeaderLibrary)
                {
                    TransitionFunction permState_MoveToNext = new TransitionFunction(
                           permState.Actual, nonheader);
                    permState_MoveToNext.DefineRange(permState.Actual, nonheader, SZMOVE_RIGHT);
                    yield return permState_MoveToNext;

                }
            }

        }

        public IEnumerable<TransitionFunction>
        Sweep_Right_XthParm_FoundNextChangeToNextState(
                    IEnumerable<State> undeterminedStates)
                    {
                        foreach (State permState in undeterminedStates)
                        {
                            // e.g. mkTFs (q1,0,1) and (q1,0,0), match q1o. So 1, and 0, are potential
                            //  transition parameters.
                            List<TransitionFunction> potentialPermStates = GetPossibleTFs(permState);
                            foreach (TransitionFunction permMatchedTF in potentialPermStates)
                            {
                                string permMatchedPotentialChar = permMatchedTF.DomainHeadValues[permState.SubScripts.Count];

                                TransitionFunction permState_TransitToNext = new TransitionFunction(
                                    permState.Actual, SymMap[permMatchedPotentialChar]);
                                string szNextState = permState.Actual + SymMap[permMatchedPotentialChar];
                                permState_TransitToNext.DefineRange(szNextState, SymMap[permMatchedPotentialChar], SZMOVE_RIGHT);

                                yield return permState_TransitToNext;

                                // Branch on _
                                if (permMatchedPotentialChar == "_")
                                {
                                    TransitionFunction permState_TransitToNextBranch = new TransitionFunction(
                                        permState.Actual, SymMap["#"]);
                                    string szNextState2 = permState.Actual + SymMap["_"];
                                    permState_TransitToNextBranch.DefineRange(szNextState2, SymMap["#"], SZMOVE_RIGHT);
                                    yield return permState_TransitToNextBranch;
                                }

                            }
                        }
                    }

        private IEnumerable<TransitionFunction>
        Sweep_Right_Complete_BeginActionStates(
                    IEnumerable<DeterminedState> determinedStates,
                    List<string> NonHeaderLibrary)
        {
            foreach (State permState in determinedStates)
            {
                foreach (string nonheader in NonHeaderLibrary)
                {
                    TransitionFunction permState_MoveToNext = new TransitionFunction(
                        permState.Actual, nonheader);
                    permState_MoveToNext.DefineRange(permState.Actual + NUM_TAPES, nonheader, SZSTAY);
                    yield return permState_MoveToNext;
                }
            }
        }

        private IEnumerable<TransitionFunction> 
        Sweep_Left_ActorState_N_MoveToPrevious(
                    IEnumerable<DeterminedState> determinedStates_ActorStates_N_Write,
                    List<string> NonHeaderLibrary)
        {
            foreach (DeterminedState dstateNthTape in determinedStates_ActorStates_N_Write)
            {

                foreach (string nonheadSymbol in NonHeaderLibrary)
                {
                    TransitionFunction determinedNState_MoveToPrevious = new TransitionFunction(
                                dstateNthTape.Actual, nonheadSymbol);

                    determinedNState_MoveToPrevious.DefineRange(
                        dstateNthTape.Actual, nonheadSymbol, SZMOVE_LEFT);

                    yield return determinedNState_MoveToPrevious;
                }

            }
        }

        private IEnumerable<TransitionFunction>
        Sweep_Left_ActorState_N_FoundPreviousWriteThenChangeToMoveHeadState(
                    IEnumerable<DeterminedState> determinedStates)
        {
            foreach (DeterminedState dstate in determinedStates)
            {
                for (int i = NUM_TAPES; i > 0; i--)
                {
                    string NthParameterOfDeterminedState = dstate.SubScripts[i - 1];
                    TransitionFunction determinedNState_FoundPreviousAndWrite = new TransitionFunction(
                                dstate.Actual + (i).ToString(), NthParameterOfDeterminedState);

                    string NthTapeTransitionWrite = dstate.TF.RangeHeadWrite[i - 1];
                    string NthTapeTransitionMove = dstate.TF.RangeHeadMove[i - 1];


                    if (NthTapeTransitionMove != SZSTAY)
                    {
                        determinedNState_FoundPreviousAndWrite.DefineRange(
                            dstate.Actual + (i).ToString() + "a", NthTapeTransitionWrite, NthTapeTransitionMove);
                    }
                    else
                    {
                        if (i > 1)
                        {
                            determinedNState_FoundPreviousAndWrite.DefineRange(
                            dstate.Actual + (i - 1), SymMap[NthTapeTransitionWrite], SZMOVE_LEFT);
                        }
                        else
                        {
                            determinedNState_FoundPreviousAndWrite.DefineRange(
                            dstate.Actual + "F", SymMap[NthTapeTransitionWrite], SZMOVE_LEFT);
                        }
                    }

                    yield return determinedNState_FoundPreviousAndWrite;

                    // Also accept H as B.
                    if (NthParameterOfDeterminedState == SymMap["_"])
                    {
                        TransitionFunction determinedNState_FoundPreviousAndWrite_H = new TransitionFunction(
                                dstate.Actual + (i).ToString(), SymMap["#"]);

                        if (NthTapeTransitionWrite != "_")
                        {
                            if (NthTapeTransitionMove != SZSTAY)
                            {
                                determinedNState_FoundPreviousAndWrite_H.DefineRange(
                                    dstate.Actual + (i).ToString() + "a", NthTapeTransitionWrite, NthTapeTransitionMove);
                            }
                            else
                            {
                                if (i > 1)
                                {
                                    determinedNState_FoundPreviousAndWrite_H.DefineRange(
                                    dstate.Actual + (i - 1), SymMap["#"], SZMOVE_LEFT);
                                }
                                else
                                {
                                    determinedNState_FoundPreviousAndWrite_H.DefineRange(
                                    dstate.Actual + "F", SymMap["#"], SZMOVE_LEFT);
                                }
                            }

                        }
                        else
                        {
                            if (NthTapeTransitionMove != SZSTAY)
                            {
                                determinedNState_FoundPreviousAndWrite_H.DefineRange(
                                    dstate.Actual + (i).ToString() + "a", "#", NthTapeTransitionMove);
                            }
                            else
                            {
                                if (i > 1)
                                {
                                    determinedNState_FoundPreviousAndWrite_H.DefineRange(
                                    dstate.Actual + (i - 1), SymMap["#"], SZMOVE_LEFT);
                                }
                                else
                                {
                                    determinedNState_FoundPreviousAndWrite_H.DefineRange(
                                    dstate.Actual + "F", SymMap["#"], SZMOVE_LEFT);
                                }
                            }


                        }
                        yield return determinedNState_FoundPreviousAndWrite_H;
                    }



                }
            }
        }

        private IEnumerable<TransitionFunction>
        Sweep_Left_ActorState_N_MoveHeadThenChangeToNLess1MoveToPrevious(
                    IEnumerable<DeterminedState> determinedStates,
                    List<string> NonHeaderLibrary)
        {
           foreach (DeterminedState dstate in determinedStates)
            {
                for (int i = NUM_TAPES; i > 0; i--)
                {
                    if (dstate.TF.RangeHeadMove[i - 1] != SZSTAY)
                    {
                        if (i > 1)
                        {
                            foreach (string character in NonHeaderLibrary)
                            {
                                // Exclude this combination because the head has moved before the beginning of the tape.
                                if (!(character == "#" && dstate.TF.RangeHeadMove[i - 1] == SZMOVE_LEFT))
                                {
                                    TransitionFunction determinedNState_MoveHead = new TransitionFunction(
                                            dstate.Actual + i + "a", character);
                                    determinedNState_MoveHead.DefineRange(
                                        dstate.Actual + (i - 1), SymMap[character], SZMOVE_LEFT);
                                    yield return determinedNState_MoveHead;
                                }

                            }
                        }
                        else
                        {
                            foreach (string character in NonHeaderLibrary)
                            {
                                // Exclude this combination because the head has moved before the beginning of the tape.
                                if (!(character == "#" && dstate.TF.RangeHeadMove[i - 1] == SZMOVE_LEFT))
                                {
                                    TransitionFunction determinedNState_MoveHead = new TransitionFunction(
                                            dstate.Actual + i + "a", character);
                                    determinedNState_MoveHead.DefineRange(
                                        dstate.Actual + "F", SymMap[character], SZSTAY);
                                    yield return determinedNState_MoveHead;
                                }
                            }
                        }
                    }
                }
            }
        }

        private IEnumerable<TransitionFunction> 
        Sweep_Left_Complete_ActorState_0F_ChangeToNext(
                    IEnumerable<DeterminedState> determinedStates_ActorStates_0F_Complete,
                    List<string> TapeLibrary)
        {
            foreach (DeterminedState permStateCompleted in determinedStates_ActorStates_0F_Complete)
            {
                foreach (string character in TapeLibrary)
                {
                    TransitionFunction completeState_MoveToBegin = new TransitionFunction(
                        permStateCompleted.Actual, character);
                    completeState_MoveToBegin.DefineRange(permStateCompleted.TF.RangeState.Actual, character, SZSTAY);
                    yield return completeState_MoveToBegin;
                }
            }
        }

        private IEnumerable<TransitionFunction>
        Include_Shift_Right_Safety(
                    IEnumerable<TransitionFunction> allTFS,
                    List<string> TapeLibrary)
        {
            List<TransitionFunction> ReplacedTFs = new List<TransitionFunction>();
            foreach (TransitionFunction tf in allTFS)
            {
                // Don't extend the virtual tape if its just going to be written with a blank...
                if (tf.DomainHeadValues[0] == SymMap["#"] &&
                    (tf.RangeHeadWrite[0] != SymMap["#"] && tf.RangeHeadWrite[0] != SymMap["_"]) &&
                    (tf.RangeHeadWrite[0] != "#" && tf.RangeHeadWrite[0] != "_"))
                {
                    ReplacedTFs.Add(tf);

                    TransitionFunction determinedNState_BranchOnPound = new TransitionFunction(
                                    tf.DomainState.Actual, SymMap["#"]);
                    determinedNState_BranchOnPound.DefineRange(
                        tf.DomainState.Actual + "S" + "#", RETURN_SYMBOL, SZMOVE_RIGHT);

                    // Start the branch
                    yield return determinedNState_BranchOnPound;

                    #region Build Shift Right TFs and Shift States

                    var TPLWD = TapeLibrary.ToList().Concat(new List<string>() { END_SYMBOL });
                    foreach (string anySymbol in TapeLibrary)
                    {
                        foreach (string targetSymbol in TPLWD)
                        {
                            // (q#, $) --> (q$, #, R)
                            if ((targetSymbol == END_SYMBOL && anySymbol == "#") ||
                                (targetSymbol != END_SYMBOL))
                            {
                                string sourceState = tf.DomainState.Actual + "S" + anySymbol;
                                TransitionFunction dstateNthTape_ShiftRight = new TransitionFunction(
                                    sourceState, targetSymbol);

                                string targetState = tf.DomainState.Actual + "S" + targetSymbol;
                                dstateNthTape_ShiftRight.DefineRange(
                                    targetState, anySymbol, SZMOVE_RIGHT);

                                yield return dstateNthTape_ShiftRight;

                            }

                        }
                    }
                    #endregion

                    #region Build Found End Shift Transition To (Move To Return to Proc) State.
                    TransitionFunction dstateNthTape_ShiftFoundEnd = new TransitionFunction(
                        tf.DomainState.Actual + "S" + END_SYMBOL, "_");

                    dstateNthTape_ShiftFoundEnd.DefineRange(
                        tf.DomainState.Actual + "S" + RETURN_SYMBOL, END_SYMBOL, SZMOVE_LEFT);
                    yield return dstateNthTape_ShiftFoundEnd;
                    #endregion

                    #region Build Move To Return to Proc TFs and Transition to Proc
                    foreach (string anySymbol in TapeLibrary)
                    {

                        TransitionFunction dstateNthTape_ReturnToProcedure = new TransitionFunction(
                           tf.DomainState.Actual + "S" + RETURN_SYMBOL, anySymbol);

                        dstateNthTape_ReturnToProcedure.DefineRange(
                            tf.DomainState.Actual + "S" + RETURN_SYMBOL, anySymbol, SZMOVE_LEFT);
                        yield return dstateNthTape_ReturnToProcedure;

                    }

                    TransitionFunction dstateNthTape_TransitionToProcedure = new TransitionFunction(
                            tf.DomainState.Actual + "S" + RETURN_SYMBOL, RETURN_SYMBOL);

                    dstateNthTape_TransitionToProcedure.DefineRange(
                        tf.DomainState.Actual, SymMap["_"], SZSTAY);
                    yield return dstateNthTape_TransitionToProcedure;
                    #endregion
                }
                else
                {
                    yield return tf;
                }


            }

        }


        private List<TransitionFunction> GetPossibleTFs(State state)
        {
            TuringMachine tm = TM;
            List<TransitionFunction> lstRetVal = new List<TransitionFunction>();

            foreach (TransitionFunction tf in tm.TransitionFunctions)
            {
                bool found = true;
                int subsLength = 0;
                foreach (string subscript in state.SubScripts)
                {
                    subsLength += subscript.Length;
                }
                found &= tf.DomainState.Actual == state.Actual.Substring(0, state.Actual.Length - subsLength);

                for (int i = 0; i < state.SubScripts.Count; i++)
                {
                    found &= SymMap[tf.DomainHeadValues[i]] == state.SubScripts[i];
                }

                if (found)
                {
                    lstRetVal.Add(tf);
                }
            }

            return lstRetVal;
        }

    }
}