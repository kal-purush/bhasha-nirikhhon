using UnityEngine;
using System.Threading;
using System.Threading.Tasks;
using System;
using System.Collections;
using System.Collections.Generic;
using Grpc.Core;
using Braincommunication;

namespace MLAgents.Sensor
{
    public class BrainServer : MonoBehaviour
    {
        public RemoteAgent remoteAgent;

        [Space(10)]
        public int lowerObservationsLength = 35;
        public int upperObservationsLength = 35;

        [Space(10)]
        public int Port = 50051;

        private Server server;

        void Start()
        {
            StartServer();
        }

        private void StartServer()
        {
            BrainCommunicatorImpl brainCommunicatorImpl = new BrainCommunicatorImpl(
                lowerObservationsLength,
                upperObservationsLength);
            brainCommunicatorImpl.remoteAgent = remoteAgent;
            
            server = new Server
                {
                    Services = { BrainCommunicator.BindService(brainCommunicatorImpl) },
                    Ports = { new ServerPort("localhost", Port, ServerCredentials.Insecure) }
                };
            server.Start();
            Debug.Log("Server started");
        }

        private void StopServer()
        {
            server.ShutdownAsync().Wait();
        }

        class BrainCommunicatorImpl : BrainCommunicator.BrainCommunicatorBase
        {
            public RemoteAgent remoteAgent;

            private float[] lowerObservations;
            private float[] upperObservations;

            public BrainCommunicatorImpl(int lowerObservationsLength, int upperObservationsLength) : base()
            {
                lowerObservations = new float[lowerObservationsLength];
                upperObservations = new float[upperObservationsLength];
            }

            public override Task<AgentAction> GetAction(Observations observations, ServerCallContext context)
            {
                var lowerObsList = new List<float>();
                lowerObsList.AddRange(observations.LowerObservations);
                var upperObsList = new List<float>();
                upperObsList.AddRange(observations.UpperObservations);
                
                // Set observations to remote agent.
                remoteAgent.SetObservations(lowerObsList.ToArray(), upperObsList.ToArray());
                remoteAgent.RequestDecision();

                int action = remoteAgent.GetDecidedAction();

                // Send remote agents action back.
                return Task.FromResult(new AgentAction { Action = action });
            }
        }
    }
}