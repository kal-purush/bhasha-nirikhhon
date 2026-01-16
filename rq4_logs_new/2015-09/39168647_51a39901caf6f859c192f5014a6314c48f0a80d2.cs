//#define DEBUG
//#define PKTDUMP 

using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;

namespace ICSimulator
{
	public class MultiMeshRouter : Router
	{
		public Router[] subrouter = new Router[Config.sub_net];

		protected override void _doStep ()
		{
			for (int i = 0; i < Config.sub_net; i++) 
				subrouter [i].doStep ();

		}

		// only determine if can or cannot inject
		public override bool canInjectFlit (Flit f)
		{
			bool can = false;
			for (int i = 0; i < Config.sub_net; i++) 
			{
				can = can | subrouter [i].canInjectFlit (f);
			}
			return can;
		}

		// if more than one subrouter can inject, select one randomly
		// or inject to the available one.
		// Otherwise throttle the injection.
		public override void InjectFlit (Flit f)
		{

			Router [] availSubrouter = new Router [Config.sub_net];
			int count = 0;

			for (int i = 0; i < Config.sub_net; i++) 
			{
				if (subrouter [i].canInjectFlit (f)) 
				{
					availSubrouter [count] = subrouter [i];
					count++;
				}
			}

			if (count <= 0)
				throw new Exception ("no subrouter is available for injection.");
			else 
			{
				int selected = Simulator.rand.Next(count);
				availSubrouter [selected].InjectFlit (f);

				#if PKTDUMP
				//	Console.WriteLine("PKT {0}-{1}/{2} aclaim the injection slot at subnet {3} router {4} at time {5}", 
				//                  f.packet.ID, f.flitNr+1, f.packet.nrOfFlits, selected, coord, Simulator.CurrentRound);
				#endif
			}

		}

		public MultiMeshRouter (Coord c)
		{
			for (int i=0; i<Config.sub_net; i++) 
			{
				subrouter [i] = makeSubRouters (c);
				subrouter [i].subnet = i;
			}
		}

		Router makeSubRouters (Coord c)
		{
			switch (Config.router.algorithm)
			{
				case RouterAlgorithm.BLESS_BYPASS:
				return new Router_BLESS_BYPASS (c);

				case RouterAlgorithm.DR_AFC:
				return new Router_AFC(c);

				case RouterAlgorithm.DR_FLIT_SWITCHED_CTLR:
				return new Router_Flit_Ctlr(c);

				case RouterAlgorithm.DR_FLIT_SWITCHED_OLDEST_FIRST:
				return new Router_Flit_OldestFirst(c);

				case RouterAlgorithm.DR_SCARAB:
				return new Router_SCARAB(c);

				case RouterAlgorithm.DR_FLIT_SWITCHED_GP:
				return new Router_Flit_GP(c);

				case RouterAlgorithm.DR_FLIT_SWITCHED_CALF:
				return new Router_SortNet_GP(c);

				case RouterAlgorithm.DR_FLIT_SWITCHED_CALF_OF:
				return new Router_SortNet_OldestFirst(c);

				case RouterAlgorithm.DR_FLIT_SWITCHED_RANDOM:
				return new Router_Flit_Random(c);

				case RouterAlgorithm.ROUTER_FLIT_EXHAUSTIVE:
				return new Router_Flit_Exhaustive(c);

				case RouterAlgorithm.OLDEST_FIRST_DO_ROUTER:
				return new OldestFirstDORouter(c);

				case RouterAlgorithm.ROUND_ROBIN_DO_ROUTER:
				return new RoundRobinDORouter(c);

				case RouterAlgorithm.STC_DO_ROUTER:
				return new STC_DORouter(c);

				default:
				throw new Exception("invalid routing algorithm " + Config.router.algorithm);
			}
		}
	}

	public class Router_BLESS_BYPASS : Router
	{

		protected Flit m_injectSlot, m_injectSlot2;
		Queue<Flit>	[] ejectBuffer;

		public Router_BLESS_BYPASS(Coord myCoord)
			: base(myCoord)
		{
			m_injectSlot = null;
			m_injectSlot2 = null;
			ejectBuffer = new Queue<Flit>[4+Config.num_bypass];
			for (int n = 0; n < 4+Config.num_bypass; n++)
				ejectBuffer[n] = new Queue<Flit>();
		}

		Flit handleGolden(Flit f)
		{
			if (f == null)
				return f;

			if (f.state == Flit.State.Normal)
				return f;

			if (f.state == Flit.State.Rescuer)
			{
				if (m_injectSlot == null)
				{
					m_injectSlot = f;
					f.state = Flit.State.Placeholder;
				}
				else
					m_injectSlot.state = Flit.State.Carrier;

				return null;
			}

			if (f.state == Flit.State.Carrier)
			{
				f.state = Flit.State.Normal;
				Flit newPlaceholder = new Flit(null, 0);
				newPlaceholder.state = Flit.State.Placeholder;

				if (m_injectSlot != null)
					m_injectSlot2 = newPlaceholder;
				else
					m_injectSlot = newPlaceholder;

				return f;
			}

			if (f.state == Flit.State.Placeholder)
				throw new Exception("Placeholder should never be ejected!");

			return null;
		}

		// accept one ejected flit into rxbuf
		protected void acceptFlit(Flit f)
		{
			statsEjectFlit(f);
			if (f.packet.nrOfArrivedFlits + 1 == f.packet.nrOfFlits)
				statsEjectPacket(f.packet);

			m_n.receiveFlit(f);
		}

		Flit ejectLocal()
		{
			// eject locally-destined flit (highest-ranked, if multiple)
			// bypass channel can also eject to local port
			Flit ret = null;			

			int bestDir = -1;
			for (int dir = 0; dir < 4; dir++)
				if (linkIn[dir] != null && linkIn[dir].Out != null &&
				    linkIn[dir].Out.state != Flit.State.Placeholder &&
				    linkIn[dir].Out.dest.ID == ID &&
				    (ret == null || rank(linkIn[dir].Out, ret) < 0))
			{
#if PKTDUMP
				if (ret != null)
					Console.WriteLine("PKT {0}-{1}/{2} EJECT FAIL router {3}/{4} at time {5}", 
				                  ret.packet.ID, ret.flitNr+1, ret.packet.nrOfFlits, coord, subnet, Simulator.CurrentRound);
#endif
				ret = linkIn[dir].Out;
				bestDir = dir;
			}

			for (int bypass = 0; bypass < Config.num_bypass; bypass++)
				if (bypassLinkIn[bypass] != null && bypassLinkIn[bypass].Out != null &&
				    bypassLinkIn[bypass].Out.state != Flit.State.Placeholder &&
				    bypassLinkIn[bypass].Out.packet.dest.ID == ID && 
				    (ret == null || rank(bypassLinkIn[bypass].Out, ret) < 0))
			{
#if PKTDUMP
				if (ret != null)
					Console.WriteLine("PKT {0}-{1}/{2} EJECT FAIL router {3}/{4} at time {5}",
					                  ret.packet.ID, ret.flitNr+1, ret.packet.nrOfFlits, coord, subnet, Simulator.CurrentRound);
#endif
				ret = bypassLinkIn[bypass].Out;
				bestDir = 4+bypass;
			}

			if (bestDir >= 4) bypassLinkIn[bestDir - 4].Out = null;
			else if (bestDir != -1) linkIn[bestDir].Out = null;


#if PKTDUMP
			if (ret != null)
				Console.WriteLine("PKT {0}-{1}/{2} EJECT from port {3} router {4}/{5} at time {6}", 
					                  ret.packet.ID, ret.flitNr+1, ret.packet.nrOfFlits, bestDir, coord, subnet, Simulator.CurrentRound);
#endif

			//ret = handleGolden(ret);

			return ret;
		}

		Flit[] input = new Flit[4+Config.num_bypass]; // keep this as a member var so we don't
		// have to allocate on every step

		protected override void _doStep()
		{

			// STEP 1: Ejection
			if (Config.EjectBufferSize != -1)
			{								
				for (int dir =0; dir < 4; dir ++)
					if (linkIn[dir] != null && linkIn[dir].Out != null && linkIn[dir].Out.packet.dest.ID == ID && ejectBuffer[dir].Count < Config.EjectBufferSize)
					{
						ejectBuffer[dir].Enqueue(linkIn[dir].Out);
						linkIn[dir].Out = null;
					}
				for (int bypass = 0; bypass < Config.num_bypass; bypass++)
					if (bypassLinkIn[bypass] != null && bypassLinkIn[bypass].Out != null && bypassLinkIn[bypass].Out.packet.dest.ID == ID && ejectBuffer[4+bypass].Count < Config.EjectBufferSize)
					{
						ejectBuffer[4+bypass].Enqueue(bypassLinkIn[bypass].Out);
						bypassLinkIn[bypass].Out = null;
					}

				int bestdir = -1;			
				for (int dir = 0; dir < 4+Config.num_bypass; dir ++)
					if (ejectBuffer[dir].Count > 0 && (bestdir == -1 || ejectBuffer[dir].Peek().injectionTime < ejectBuffer[bestdir].Peek().injectionTime))
						bestdir = dir;				
				if (bestdir != -1)
					acceptFlit(ejectBuffer[bestdir].Dequeue());
			}
			else 
			{
				int flitsTryToEject = 0;
				for (int dir = 0; dir < 4; dir ++)
					if (linkIn[dir] != null && linkIn[dir].Out != null && linkIn[dir].Out.dest.ID == ID)
				{
					flitsTryToEject ++;
					if (linkIn[dir].Out.ejectTrial == 0)
						linkIn[dir].Out.firstEjectTrial = Simulator.CurrentRound;
					linkIn[dir].Out.ejectTrial ++;
#if PKTDUMP
					Console.WriteLine("PKT {0}-{1}/{2} TRY to EJECT from port {3} router {4}/{5} at time {6}", 
					                  linkIn[dir].Out.packet.ID, linkIn[dir].Out.flitNr+1, linkIn[dir].Out.packet.nrOfFlits, dir,
					                  coord, subnet, Simulator.CurrentRound);
#endif
				}
				for (int bypass = 0; bypass < Config.num_bypass; bypass++)
					if (bypassLinkIn[bypass] != null && bypassLinkIn[bypass].Out != null && bypassLinkIn[bypass].Out.packet.dest.ID == ID)
				{
					flitsTryToEject ++;
					if (bypassLinkIn[bypass].Out.ejectTrial == 0)
						bypassLinkIn[bypass].Out.firstEjectTrial = Simulator.CurrentRound;
					bypassLinkIn[bypass].Out.ejectTrial++;
#if PKTDUMP
					Console.WriteLine("PKT {0}-{1}/{2} TRY to EJECT from Bypass port {3} router {4}/{5} at time {6}", 
					                  bypassLinkIn[bypass].Out.packet.ID, bypassLinkIn[bypass].Out.flitNr+1, bypassLinkIn[bypass].Out.packet.nrOfFlits, bypass,
					                  coord, subnet, Simulator.CurrentRound);
#endif
				}

				Simulator.stats.flitsTryToEject[flitsTryToEject].Add();            

				Flit f1 = null,f2 = null;
				for (int i = 0; i < Config.meshEjectTrial; i++)
				{
					// Only support dual ejection (MAX.Config.meshEjectTrial = 2)
					Flit eject = ejectLocal();
					if (i == 0) f1 = eject; 
					else if (i == 1) f2 = eject;
					if (eject != null)             
						acceptFlit(eject); 	// Eject flit			
				}
				if (f1 != null && f2 != null && f1.packet == f2.packet)
					Simulator.stats.ejectsFromSamePacket.Add(1);
				else if (f1 != null && f2 != null)
					Simulator.stats.ejectsFromSamePacket.Add(0);
			}

			//STEP 2 : Prioritize and Injection
			for (int i = 0; i < 4+Config.num_bypass; i++) input[i] = null;
			// grab inputs into a local array so we can sort
			int c = 0;
			for (int dir = 0; dir < 4; dir++)
				if (linkIn[dir] != null && linkIn[dir].Out != null)
			{
				linkIn[dir].Out.inDir = dir;
				input[c++] = linkIn[dir].Out;  // c: # of incoming flits
				//linkIn[dir].Out.inDir = dir;  // By Xiyue: what's the point? Seems redundant
				linkIn[dir].Out = null;
			}
			for (int bypass = 0; bypass < Config.num_bypass; bypass++)
				if (bypassLinkIn[bypass] != null && bypassLinkIn[bypass].Out != null)
			{
				bypassLinkIn[bypass].Out.inDir = 4+bypass;
				input[c++] = bypassLinkIn[bypass].Out;
				bypassLinkIn[bypass].Out = null;
			}

			// sometimes network-meddling such as flit-injection can put unexpected
			// things in outlinks...
			// TODO: outCount: # of the unexpected outstanding flits at the inport of output link, usually, it is 0 (verify is later)
			int outCount = 0;
			for (int dir = 0; dir < 4; dir++)
				if (linkOut[dir] != null && linkOut[dir].In != null)
					outCount++;
			for (int j = 0; j < Config.num_bypass; j++)
				if (bypassLinkOut[j] != null && bypassLinkOut[j].In != null)
					outCount++;
			if (outCount != 0) throw new Exception("Unexpected flit on output!");
		
			bool wantToInject = m_injectSlot2 != null || m_injectSlot != null;
			bool canInject = (c + outCount) < neighbors;
			bool starved = wantToInject && !canInject;

			if (starved)
			{
				Flit starvedFlit = null;
				if (starvedFlit == null) starvedFlit = m_injectSlot2;
				if (starvedFlit == null) starvedFlit = m_injectSlot;

#if PKTDUMP
				Console.WriteLine("PKT {0}-{1}/{2} is STARVED at router {3}/{4} at time {5}",
				                  starvedFlit.packet.ID, starvedFlit.flitNr+1, starvedFlit.packet.nrOfFlits, coord, subnet, Simulator.CurrentRound);
#endif
				Simulator.controller.reportStarve(coord.ID);
				statsStarve(starvedFlit);
			}
			if (canInject && wantToInject)
			{				
				Flit inj_peek=null; 
				if(m_injectSlot2!=null)
					inj_peek=m_injectSlot2;
				else if (m_injectSlot!=null)
					inj_peek=m_injectSlot;
				if(inj_peek==null)
					throw new Exception("Inj flit peek is null!!");

				if(!Simulator.controller.ThrottleAtRouter || Simulator.controller.tryInject(coord.ID))
				{
					Flit inj = null;
					if (m_injectSlot2 != null)
					{
						inj = m_injectSlot2;
						m_injectSlot2 = null;
					}
					else if (m_injectSlot != null)
					{
						inj = m_injectSlot;
						m_injectSlot = null;
					}
					else
						throw new Exception("what???inject null flits??");

					inj.inDir = 4+Config.num_bypass;
					input[c++] = inj;
#if PKTDUMP
					Console.WriteLine("PKT {0}-{1}/{2} is INJECTED at router {3}/{4} at time {5}", 
					                  inj.packet.ID, inj.flitNr+1, inj.packet.nrOfFlits, coord, subnet, Simulator.CurrentRound);
#endif
					statsInjectFlit(inj);
				}
			}


			// inline bubble sort is faster for this size than Array.Sort()
			// sort input[] by descending priority. rank(a,b) < 0 if f0 has higher priority.
			for (int i = 0; i < 4+Config.num_bypass; i++)
				for (int j = i + 1; j < 4+Config.num_bypass; j++)
					if (input[j] != null &&
					    (input[i] == null ||
					 rank(input[j], input[i]) < 0))
				{
					Flit t = input[i];
					input[i] = input[j];
					input[j] = t;
				}

			// assign outputs
			for (int i = 0; i < 4+Config.num_bypass && input[i] != null; i++)
			{

				PreferredDirection pd = determineDirection(input[i], coord);
				#if PKTDUMP
				if (pd.xDir != Simulator.DIR_NONE)
					Console.WriteLine("PKT {0}-{1}/{2} PDir={7} ARRIVES at port {3} router {4}/{5} at time {6}",
				                  input[i].packet.ID, input[i].flitNr+1, input[i].packet.nrOfFlits, input[i].inDir, 
				                  coord, subnet, Simulator.CurrentRound, pd.xDir);
				if (pd.yDir != Simulator.DIR_NONE)
					Console.WriteLine("PKT {0}-{1}/{2} PDir={7} ARRIVES at port {3} router {4}/{5} at time {6}",
					                  input[i].packet.ID, input[i].flitNr+1, input[i].packet.nrOfFlits, input[i].inDir, 
					                  coord, subnet, Simulator.CurrentRound, pd.yDir);
				#endif

				int outDir = -1;
				bool deflect = false;
				bool bypass = false;
				if (Config.RandOrderRouting)
				{
					int dimensionOrder = Simulator.rand.Next(2);  //0: x over y   1:y over x
					if (dimensionOrder == 0)
					{
						if (pd.xDir != Simulator.DIR_NONE && linkOut[pd.xDir].In == null)
						{
							linkOut[pd.xDir].In = input[i];
							outDir = pd.xDir;
						}
						else if (pd.yDir != Simulator.DIR_NONE && linkOut[pd.yDir].In == null)
						{
							linkOut[pd.yDir].In = input[i];
							outDir = pd.yDir;
						}
						else 
							bypass = true;
					}
					else       //y over x
					{
						if (pd.yDir != Simulator.DIR_NONE && linkOut[pd.yDir].In == null)
						{
							linkOut[pd.yDir].In = input[i];
							outDir = pd.yDir;
						}
						else if (pd.xDir != Simulator.DIR_NONE && linkOut[pd.xDir].In == null)
						{
							linkOut[pd.xDir].In = input[i];
							outDir = pd.xDir;
						}
						else 
							bypass = true;
					}
				}
				else if (Config.DeflectOrderRouting)
				{
					if (input[i].routingOrder == false)
					{
						if (pd.xDir != Simulator.DIR_NONE && linkOut[pd.xDir].In == null)
						{
							linkOut[pd.xDir].In = input[i];
							linkOut[pd.xDir].In.routingOrder = false;
							outDir = pd.xDir;
						}
						else if (pd.yDir != Simulator.DIR_NONE && linkOut[pd.yDir].In == null)
						{
							linkOut[pd.yDir].In = input[i];
							linkOut[pd.yDir].In.routingOrder = true;
							outDir = pd.yDir;
						}
						else 
							bypass = true;
					}
					else       //y over x
					{
						if (pd.yDir != Simulator.DIR_NONE && linkOut[pd.yDir].In == null)
						{
							linkOut[pd.yDir].In = input[i];
							linkOut[pd.yDir].In.routingOrder = true;
							outDir = pd.yDir;
						}
						else if (pd.xDir != Simulator.DIR_NONE && linkOut[pd.xDir].In == null)
						{
							linkOut[pd.xDir].In = input[i];
							linkOut[pd.xDir].In.routingOrder = false;
							outDir = pd.xDir;
						}
						else 
							bypass = true;
					}


				}
				else //original Router
				{
					if (pd.xDir != Simulator.DIR_NONE && linkOut[pd.xDir].In == null)
					{
						linkOut[pd.xDir].In = input[i];
						outDir = pd.xDir;
					}
					else if (pd.yDir != Simulator.DIR_NONE && linkOut[pd.yDir].In == null)
					{
						linkOut[pd.yDir].In = input[i];
						outDir = pd.yDir;
					}
					else 
						bypass = true;
				}

				if (bypass)
				{
					for (int j = 0; j < Config.num_bypass; j ++)
					{
						if (bypassLinkOut[j] != null && bypassLinkOut[j].In == null && Config.bypass_enable)
						{
							input[i].Bypassed = true;
							bypassLinkOut[j].In = input[i];
							outDir = 4+j;
							break;
						}
						else
							deflect = true;
					}

				}

				// deflect!
				if (deflect)
				{
					input[i].Deflected = true;
					int dir = 0;
					if (Config.randomize_defl) dir = Simulator.rand.Next(4); // randomize deflection dir (so no bias)
					for (int count = 0; count < 4; count++, dir = (dir + 1) % 4)
						if (linkOut[dir] != null && linkOut[dir].In == null  && outDir == -1) // use outDir to determine if a flit takes bypass port
					{
						linkOut[dir].In = input[i];
						outDir = dir;
						// once a flit is deflected, it must try to route along the same dimension.
						if (dir == 0 || dir ==2)
							linkOut[dir].In.routingOrder = false;
						else
							linkOut[dir].In.routingOrder = true;
						break;
					}

				}

				
				#if PKTDUMP
				if (bypass != true && deflect != true)
					Console.WriteLine("PKT {0}-{1}/{2} TAKE PROD Port {3} router {4}/{5} at time {6}",
					                  input[i].packet.ID, input[i].flitNr+1, input[i].packet.nrOfFlits, outDir, coord, subnet, Simulator.CurrentRound);
				else if (bypass == true && deflect != true)
					Console.WriteLine("PKT {0}-{1}/{2} TAKE BYPASS Port {3} router {4}/{5} at time {6}",
					                  input[i].packet.ID, input[i].flitNr+1, input[i].packet.nrOfFlits, outDir-4, coord, subnet, Simulator.CurrentRound);
				else if (deflect == true)
					Console.WriteLine("PKT {0}-{1}/{2} DEFLECTED to Port {3} router {4}/{5} at time {6}",
					                  input[i].packet.ID, input[i].flitNr+1, input[i].packet.nrOfFlits, outDir, coord, subnet, Simulator.CurrentRound);
				#endif
				if (outDir == -1) throw new Exception(
					String.Format("Ran out of outlinks in arbitration at node {0} on input {1} cycle {2} flit {3} c {4} neighbors {5} outcount {6}", coord, i, Simulator.CurrentRound, input[i], c, neighbors, outCount));
			} // end assign output
		}

		public override bool canInjectFlit(Flit f)
		{
			return m_injectSlot == null;
		}

		public override void InjectFlit(Flit f)
		{
			if (m_injectSlot != null)
				throw new Exception("Trying to inject twice in one cycle");

			m_injectSlot = f;
		}

		public override void flush()
		{
			m_injectSlot = null;
		}

		protected virtual bool needFlush(Flit f) { return false; }



		public override int rank(Flit f1, Flit f2)
		{
			if (f1 == null && f2 == null) return 0;
			if (f1 == null) return 1;
			if (f2 == null) return -1;

			bool f1_resc = (f1.state == Flit.State.Rescuer) || (f1.state == Flit.State.Carrier);
			bool f2_resc = (f2.state == Flit.State.Rescuer) || (f2.state == Flit.State.Carrier);
			bool f1_place = (f1.state == Flit.State.Placeholder);
			bool f2_place = (f2.state == Flit.State.Placeholder);

			int c0 = 0;
			if (f1_resc && f2_resc)
				c0 = 0;
			else if (f1_resc)
				c0 = -1;
			else if (f2_resc)
				c0 = 1;
			else if (f1_place && f2_place)
				c0 = 0;
			else if (f1_place)
				c0 = 1;
			else if (f2_place)
				c0 = -1;

			int c1 = 0, c2 = 0;
			if (f1.packet != null && f2.packet != null)
			{
				c1 = -age(f1).CompareTo(age(f2));
				c2 = f1.packet.ID.CompareTo(f2.packet.ID);
			}

			int c3 = f1.flitNr.CompareTo(f2.flitNr);

			int zerosSeen = 0;
			foreach (int i in new int[] { c0, c1, c2, c3 })
			{
				if (i == 0)
					zerosSeen++;
				else
					break;
			}
			Simulator.stats.net_decisionLevel.Add(zerosSeen);
			return
				(c0 != 0) ? c0 :
				(c1 != 0) ? c1 :
				(c2 != 0) ? c2 :
				c3;
		}

		public ulong age(Flit f)
		{
			if (Config.net_age_arbitration)
				return Simulator.CurrentRound - f.packet.injectionTime;
			else
				return (Simulator.CurrentRound - f.packet.creationTime) /
					(ulong)Config.cheap_of;
		}

	}
}