using System;
using System.Collections.Generic;
using System.Text;
using PAT.Common.Classes.Expressions.ExpressionClass;
using System.Diagnostics.Contracts; 

//the namespace must be PAT.Lib, the class and method names can be arbitrary
namespace PAT.Lib
{
    /// <summary>
    /// The math library that can be used in your model.
    /// all methods should be declared as public static.
    /// 
    /// The parameters must be of type "int", or "int array"
    /// The number of parameters can be 0 or many
    /// 
    /// The return type can be bool, int or int[] only.
    /// 
    /// The method name will be used directly in your model.
    /// e.g. call(max, 10, 2), call(dominate, 3, 2), call(amax, [1,3,5]),
    /// 
    /// Note: method names are case sensetive
    /// </summary>
    public class Voter 
    {
		public int ID;
		public int Intension = -1;
        public int Code = -1;
		
		public Voter(int id)
		{
			ID = id;
		}

        public void SetCode(int code)
        {
            Contract.Ensures(Code == -1, "Code is already set with value: " + Code);
            Code = code;
        }

        public void SetIntension(int intend)
        {
            Contract.Ensures(Intension == -1, "Intension is already initialized with value: " + Intension);
            Intension = intend;
        }
		
        /// <summary>
        /// Please implement this method to provide the string representation of the datatype
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return "Voter:" +ID.ToString() + ";Intension:" + Intension +";Code:"+Code;
        }

        public int GetID()
        {
            return ID;
        }


        /// <summary>
        /// Please implement this method to return a deep clone of the current object
        /// </summary>
        /// <returns></returns>
        public Voter GetClone()
        {
            Voter v = new Voter(ID);
            v.Intension = Intension;
            v.Code = Code;
            return v;
        }


        /// <summary>
        /// Please implement this method to provide the compact string representation of the datatype
        /// </summary>
        /// <returns></returns>
        public string ExpressionID
        {
            get { return ID.ToString(); }
        }

    }
    
    
    /// <summary>
    /// The math library that can be used in your model.
    /// all methods should be declared as public static.
    /// 
    /// The parameters must be of type "int", or "int array"
    /// The number of parameters can be 0 or many
    /// 
    /// The return type can be bool, int or int[] only.
    /// 
    /// The method name will be used directly in your model.
    /// e.g. call(max, 10, 2), call(dominate, 3, 2), call(amax, [1,3,5]),
    /// 
    /// Note: method names are case sensetive
    /// </summary>
    public class BallotCollection : ExpressionValue
    {
        private int VotersNo;
        private int CandidatesNo;

        public string[][] TableP;
        public bool[][] TableS;
        public string[][] TableQ;
        public KeyValuePair<int,int>[][] TableR;

        public Ballot[] Ballots;
        public Voter[] Voters;

        public BallotCollection(int voterNo, int candidateNo)
        {
            VotersNo = voterNo;
            CandidatesNo = candidateNo;

            TableInitialization();

            Ballots = new Ballot[voterNo];
            Voters = new Voter[voterNo];


            for (int i = 0; i < VotersNo; i++)
            {
                int[] candidates = new int[candidateNo];
                for (int j = 0; j < candidateNo; j++)
                {
                    candidates[j] = j; // SwitchBoardMapping2[i + candidateNo + j].Value;
                }
                Ballots[i] = new Ballot(i, candidates);  
                Voters[i] = new Voter(i);
            }
        }

        private void TableInitialization()
        {
            
        }


        [ContractInvariantMethod]
        protected void ObjectInvariant()
        {
            Contract.Invariant(Contract.ForAll(0, VotersNo, index => (Voters[index].Intension > -1 && Voters[index].Intension < CandidatesNo)));
        }

        public BallotCollection(int voterNo, int candidateNo, Ballot[] ballots, Voter[] voters)
        {
            VotersNo = voterNo;
            CandidatesNo = candidateNo;

            Ballots = new Ballot[voterNo];
            Voters = new Voter[voterNo];

            for (int i = 0; i < VotersNo; i++)
            {
                Ballots[i] = ballots[i].GetClone();
                Voters[i] = voters[i].GetClone();
            }
        }

        public void SetIntension(int v, int intend)
        {
            Voters[v].SetIntension(intend);
        }

        public void Vote(int v)
        {
            int code = Ballots[v].Vote(v, Voters[v].ID);
            Voters[v].SetCode(code);
        }

        public int Scan(int v, bool correct)
        {
           return Ballots[v].Scan(correct);
        }

        public bool Challenge(int v)
        {
            return Voters[v].Code == Ballots[v].Choice;
        }

        /// <summary>
        /// Please implement this method to provide the string representation of the datatype
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("Voters:");
            foreach (Voter voter in Voters)
            {
                sb.AppendLine(voter.ToString());
            }
            sb.AppendLine("Ballots:");
            foreach (Ballot ballot in Ballots)
            {
                sb.AppendLine(ballot.ToString());
            }
            return sb.ToString();
        }


        /// <summary>
        /// Please implement this method to return a deep clone of the current object
        /// </summary>
        /// <returns></returns>
        public override ExpressionValue GetClone()
        {
            BallotCollection b = new BallotCollection(VotersNo, CandidatesNo, Ballots, Voters);
            return b;
        }


        /// <summary>
        /// Please implement this method to provide the compact string representation of the datatype
        /// </summary>
        /// <returns></returns>
        public override string ExpressionID
        {
            get
            {
                StringBuilder sb = new StringBuilder();
                foreach (Ballot ballot in Ballots)
                {
                    sb.AppendLine(ballot.ExpressionID);
                }
                return sb.ToString();
            }
        }



    }

    
    
    /// <summary>
    /// The math library that can be used in your model.
    /// all methods should be declared as public static.
    /// 
    /// The parameters must be of type "int", or "int array"
    /// The number of parameters can be 0 or many
    /// 
    /// The return type can be bool, int or int[] only.
    /// 
    /// The method name will be used directly in your model.
    /// e.g. call(max, 10, 2), call(dominate, 3, 2), call(amax, [1,3,5]),
    /// 
    /// Note: method names are case sensetive
    /// </summary>
    public class Ballot
    {
		private int ID;
		public int Voter = -1;
		private int[] Candidates;
		
		//index of the candidates;
		public int Choice = -1;
		
		//this is the choice scanned in 
		public int ScannedCandidate = -1;
		
		public Ballot(int id, int[] candidates)
		{
			ID = id;
			Candidates = candidates;
		}
		
		public int Vote(int v, int intend)
		{
			Voter = v;
			for(int i = 0; i <Candidates.Length; i++)
			{
				if(Candidates[i] == intend)
				{
					Choice = i;
				    return Choice;
				}
			}

            throw new RuntimeException("There is no candidate of ID: " + intend);
		}
		
		public int Scan(bool correct)
		{
			//if scanned correctly. then the 
			if(correct)
			{
                ScannedCandidate = Candidates[Choice];
			}
			else			
			{
				//if the scan is the last one, then set to the previous one
                if (Choice == Candidates.Length - 1)
				{
                    ScannedCandidate = Candidates[Choice - 1];	
				}
				//otherwise set to the next one
				else
				{
                    ScannedCandidate = Candidates[Choice + 1];
				}
			}

		    return ScannedCandidate;
		}
		
		
        /// <summary>
        /// Please implement this method to provide the string representation of the datatype
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return "Ballot:" + ID + "; VoterID:" + Voter + " Choice:" + Choice + ": Scan:" + ScannedCandidate;
        }


        /// <summary>
        /// Please implement this method to return a deep clone of the current object
        /// </summary>
        /// <returns></returns>
        public Ballot GetClone()
        {
            Ballot b = new Ballot(ID, Candidates);
            b.Voter = Voter;
            b.Choice = Choice;
            b.ScannedCandidate = ScannedCandidate;
            return b;
        }


        /// <summary>
        /// Please implement this method to provide the compact string representation of the datatype
        /// </summary>
        /// <returns></returns>
        public string ExpressionID
        {
            get {
            	return ID + " " + Voter + " " + Choice + " " + ScannedCandidate;
            }
        }

    }
    
    
}