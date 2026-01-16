using Abstractions;
using Abstractions.Commands;
using UnityEngine;
using UserControlSystem;
using UserControlSystem.Models;
using UserControlSystem.Models.CommandCreators;
using Utils;
using Zenject;

[CreateAssetMenu(fileName = "AssetsInstaller", menuName = "Installers/AssetsInstaller")]
public class AssetsInstaller: ScriptableObjectInstaller<AssetsInstaller>
{
    [SerializeField] private AssetsContext _legacyContext;
    
    [SerializeField] private Vector3Value _groundClickRMB;
    [SerializeField] private SelectableValue _selectable;
    [SerializeField] private AttackableValue _attackableClickRMB;

    public override void InstallBindings()
    {
        Container.BindInstances(_legacyContext, _selectable, _groundClickRMB, _attackableClickRMB);
        
        Container.Bind<IAwaitable<IAttackable>>().FromInstance(_attackableClickRMB);
        Container.Bind<IAwaitable<Vector3>>().FromInstance(_groundClickRMB);
        Container.Bind<IAwaitable<ISelectable>>().FromInstance(_selectable);
        
        Container.Bind<CommandCreatorBase<IProduceUnitCommand>>().To<ProduceUnitCommandCreator>().AsTransient();
        Container.Bind<CommandCreatorBase<IMoveCommand>>().To<MoveCommandCreator>().AsTransient();
        Container.Bind<CommandCreatorBase<IAttackCommand>>().To<AttackCommandCreator>().AsTransient();
        Container.Bind<CommandCreatorBase<IPatrolCommand>>().To<PatrolCommandCreator>().AsTransient();
        Container.Bind<CommandCreatorBase<IStopCommand>>().To<StopCommandCreator>().AsTransient();
            
        Container.Bind<CommandButtonsModel>().AsTransient();
    }
}