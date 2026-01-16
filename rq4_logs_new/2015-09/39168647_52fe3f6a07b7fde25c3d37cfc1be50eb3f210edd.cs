using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace ICSimulator
{
	public class MultiMeshRouter : Router
	{
		public Router[] subrouter = new Router[Config.sub_net];

		protected override void _doStep ()
		{

		}

		public override bool canInjectFlit (Flit f)
		{
			return false;
		}

		public override void InjectFlit (Flit f)
		{

		}

		public MultiMeshRouter (Coord c)
		{
			for (int i=0; i<Config.sub_net; i++) 
			{
				subrouter [i] = makeSubRouters (c);
			}
		}


		Router makeSubRouters (Coord c)
		{
			switch (Config.router.algorithm)
			{
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


	public class MultiMesh : Network
	{
		private MultiMeshRouter[] _routers;

		public MultiMesh (int dimX, int dimY) : base(dimX, dimY) {	}

		public override void setup()
		{
			if (Config.sub_net <= 0) 
				throw new Exception ("No subnetwork is configured (sub_net <= 0)!");

			_routers = new MultiMeshRouter [Config.N];
			nodes = new Node[Config.N];
			links = new List<Link>();
			cache = new CmpCache();

			endOfTraceBarrier = new bool[Config.N];
			canRewind = false;

			ParseFinish(Config.finish);

			workload = new Workload(Config.traceFilenames);

			mapping = new NodeMapping_AllCPU_SharedCache();

			// create routers and nodes
			// each node contains router [sub_net * node, ... ,  sub_net*(node+1)-1]
			for (int n = 0; n < Config.N; n++)
			{
				Coord c = new Coord(n);
				nodes[n] = new Node(mapping, c);
				_routers [n] = new MultiMeshRouter (c);
				nodes [n].setRouter (_routers [n]);
				_routers [n].setNode (nodes [n]);
				endOfTraceBarrier[n] = false;
			}

			// create the Golden manager
			// NOT sure if needed
			golden = new Golden();

			// connect the network with Links
			for (int n = 0; n < Config.N; n++)
			{
				int x, y;
				Coord.getXYfromID(n, out x, out y);

				// inter-router links
				for (int dir = 0; dir < 4; dir++)
				{

					// Clockwise 0->3 map to N->E->S->W
					/* Coordinate Mapping (e.g. 16 nodes)
					 * (0,3) (1,3) .....     |||||    3  7 ...
					 * ...					 |||||	  2  6 ...
					 * ...					 |||||    1  5 ...
					 * (0,0) (1,0) ......	 |||||    0  4 ...
					 * */

					int oppDir = (dir + 2) % 4; // direction from neighbor's perspective

					// determine neighbor's coordinates
					int x_, y_;
					switch (dir)
					{
						case Simulator.DIR_UP: x_ = x; y_ = y + 1; break;
						case Simulator.DIR_DOWN: x_ = x; y_ = y - 1; break;
						case Simulator.DIR_RIGHT: x_ = x + 1; y_ = y; break;
						case Simulator.DIR_LEFT: x_ = x - 1; y_ = y; break;
						default: continue;
					}

					// If we are a torus, we manipulate x_ and y_
					if(Config.torus)
					{
						if(x_ < 0)
							x_ += X;
						else if(x_ >= X)
							x_ -= X;

						if(y_ < 0)
							y_ += Y;
						else if(y_ >= Y)
							y_ -= Y;
					}
					// mesh, not torus: detect edge
					else if (x_ < 0 || x_ >= X || y_ < 0 || y_ >= Y)
						continue;

					// ensure no duplication by handling a link at the lexicographically
					// first router
					if (x_ < x || (x_ == x && y_ < y)) continue;

					int ID, ID_neighbor; 
					ID = Coord.getIDfromXY(x, y);
					ID_neighbor = Coord.getIDfromXY(x_, y_);

					for (int m = 0; m < Config.sub_net; m++)
					{
						// Link param is *extra* latency (over the standard 1 cycle)
						Link dirA = new Link(Config.router.linkLatency - 1);
						Link dirB = new Link(Config.router.linkLatency - 1);
						links.Add(dirA);
						links.Add(dirB);

						// link 'em up
						routers[ID].linkOut[dir] = dirA;
						routers[ID_neighbor].linkIn[oppDir] = dirA;

						routers[ID].linkIn[dir] = dirB;
						routers[ID_neighbor].linkOut[oppDir] = dirB;

						routers[ID].neighbors++;
						routers[ID_neighbor].neighbors++;

						routers[ID].neigh[dir] = routers[ID_neighbor];
						routers[ID_neighbor].neigh[oppDir] = routers[ID];

					}
				}
			}

			if (Config.torus)
				for (int i = 0; i < Config.N; i++)
					if (routers[i].neighbors < 4)
						throw new Exception("torus construction not successful!");
		}

		public override void doStep()
		{

			doStats();

			// step the golden controller
			// golden.doStep();

			// step the nodes

			for (int n = 0; n < Config.N; n++)
				nodes[n].doStep();
			// step the network sim: first, routers
				
				for (int n = 0; n < Config.N; n++)	
					routers[n].doStep();
	
			// now, step each link
			foreach (Link l in links)
				l.doStep();
		}

	}



}