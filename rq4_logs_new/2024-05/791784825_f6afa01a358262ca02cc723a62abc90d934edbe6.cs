using UnityEngine;
using UnityHFSM;
using LlamAcademy.Sensors;
using UnityEngine.AI;

namespace LlamAcademy.FSM
{
    [RequireComponent(typeof(Animator), typeof(NavMeshAgent))]
    public class Enemy : MonoBehaviour
    {
        [Header("References")]
        [SerializeField]
        private Player Player;

        [Header("Attack Config")]
        [SerializeField]
        [Range(0.1f, 5f)]
        private float AttackCooldown = 2;

        [Header("Sensors")]
        [SerializeField]
        private PlayerSensor FollowPlayerSensor;
        [SerializeField]
        private PlayerSensor MeleePlayerSensor;

        [Space]
        [Header("Debug Info")]
        [SerializeField]
        private bool IsInMeleeRange;
        [SerializeField]
        private bool IsInChasingRange;
        [SerializeField]
        private float LastAttackTime;

        private StateMachine<EnemyState, StateEvent> EnemyFSM;
        private NavMeshAgent Agent;

        private void Awake()
        {
            Agent = GetComponent<NavMeshAgent>();
            EnemyFSM = new();

            // Add States
            EnemyFSM.AddState(EnemyState.Idle, new IdleState(false, this));
            EnemyFSM.AddState(EnemyState.Chase, new ChaseState(true, this, Player.transform));
            EnemyFSM.AddState(EnemyState.Attack, new AttackState(true, this, OnAttack));

            // Add Transitions
            EnemyFSM.AddTriggerTransition(StateEvent.DetectPlayer, new Transition<EnemyState>(EnemyState.Idle, EnemyState.Chase));
            EnemyFSM.AddTriggerTransition(StateEvent.LostPlayer, new Transition<EnemyState>(EnemyState.Chase, EnemyState.Idle));
            EnemyFSM.AddTransition(new Transition<EnemyState>(EnemyState.Idle, EnemyState.Chase,
                (transition) => IsInChasingRange
                                && Vector3.Distance(Player.transform.position, transform.position) > Agent.stoppingDistance)
            );
            EnemyFSM.AddTransition(new Transition<EnemyState>(EnemyState.Chase, EnemyState.Idle,
                (transition) => !IsInChasingRange
                                || Vector3.Distance(Player.transform.position, transform.position) <= Agent.stoppingDistance)
            );

            // Attack Transitions
            EnemyFSM.AddTransition(new Transition<EnemyState>(EnemyState.Chase, EnemyState.Attack, ShouldMelee));
            EnemyFSM.AddTransition(new Transition<EnemyState>(EnemyState.Idle, EnemyState.Attack, ShouldMelee));
            EnemyFSM.AddTransition(new Transition<EnemyState>(EnemyState.Attack, EnemyState.Chase, IsNotWithinIdleRange));
            EnemyFSM.AddTransition(new Transition<EnemyState>(EnemyState.Attack, EnemyState.Idle, IsWithinIdleRange));

            EnemyFSM.Init();
        }

        private void Start()
        {
            FollowPlayerSensor.OnPlayerEnter += FollowPlayerSensor_OnPlayerEnter;
            FollowPlayerSensor.OnPlayerExit += FollowPlayerSensor_OnPlayerExit;
            MeleePlayerSensor.OnPlayerEnter += MeleePlayerSensor_OnPlayerEnter;
            MeleePlayerSensor.OnPlayerExit += MeleePlayerSensor_OnPlayerExit;
        }

        private void FollowPlayerSensor_OnPlayerExit(Vector3 LastKnownPosition)
        {
            EnemyFSM.Trigger(StateEvent.LostPlayer);
            IsInChasingRange = false;
        }

        private void FollowPlayerSensor_OnPlayerEnter(Transform Player)
        {
            EnemyFSM.Trigger(StateEvent.DetectPlayer);
            IsInChasingRange = true;
        }

        private bool ShouldMelee(Transition<EnemyState> Transition) =>
            LastAttackTime + AttackCooldown <= Time.time
                   && IsInMeleeRange;

        private bool IsWithinIdleRange(Transition<EnemyState> Transition) =>
            Agent.remainingDistance <= Agent.stoppingDistance;

        private bool IsNotWithinIdleRange(Transition<EnemyState> Transition) =>
            !IsWithinIdleRange(Transition);

        private void MeleePlayerSensor_OnPlayerExit(Vector3 LastKnownPosition) => IsInMeleeRange = false;

        private void MeleePlayerSensor_OnPlayerEnter(Transform Player) => IsInMeleeRange = true;


        private void OnAttack(State<EnemyState, StateEvent> State)
        {
            transform.LookAt(Player.transform.position);
            LastAttackTime = Time.time;
        }

        private void Update()
        {
            EnemyFSM.OnLogic();
        }
    }
}