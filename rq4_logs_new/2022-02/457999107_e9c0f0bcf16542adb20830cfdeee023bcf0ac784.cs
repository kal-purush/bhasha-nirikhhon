using System;
using System.Runtime.InteropServices;
using Abstractions;
using Abstractions.Commands;
using UserControlSystem.Models.CommandCreators;
using Zenject;

namespace UserControlSystem.Models
{
    public sealed class CommandButtonsModel
    {
        public event Action<ICommandExecutor> OnCommandAccepted;
        public event Action OnCommandComplete;
        public event Action OnCommandCancel;

        [Inject] private CommandCreatorBase<IProduceUnitCommand> _unitProducer;
        [Inject] private CommandCreatorBase<IAttackCommand> _attacker;
        [Inject] private CommandCreatorBase<IMoveCommand> _moving;
        [Inject] private CommandCreatorBase<IPatrolCommand> _patrolling;
        [Inject] private CommandCreatorBase<IStopCommand> _stopped;

        private bool _commandIsPending;

        public void OnCommandButtonClick(ICommandExecutor commandExecutor)
        {
            if (_commandIsPending)
            {
                OnProcessCancel();
            }

            _commandIsPending = true;
            OnCommandAccepted?.Invoke(commandExecutor);
            
            _unitProducer.ProcessCommandExecutor(commandExecutor,
                command => ExecuteCommandWrapper(commandExecutor, command));
            
            _attacker.ProcessCommandExecutor(commandExecutor,
                command => ExecuteCommandWrapper(commandExecutor, command));
            _moving.ProcessCommandExecutor(commandExecutor,
                command => ExecuteCommandWrapper(commandExecutor, command));
            _patrolling.ProcessCommandExecutor(commandExecutor,
                command => ExecuteCommandWrapper(commandExecutor, command));
            _stopped.ProcessCommandExecutor(commandExecutor,
                command => ExecuteCommandWrapper(commandExecutor, command));
        }

        public void OnSelectionChanged()
        {
            _commandIsPending = false;
            OnProcessCancel();
        }

        public void OnProcessCancel()
        {
            _unitProducer.ProcessCancel();
            
            _attacker.ProcessCancel();
            _moving.ProcessCancel();
            _patrolling.ProcessCancel();
            _stopped.ProcessCancel();
            
            OnCommandCancel?.Invoke();
        }
        
        private void ExecuteCommandWrapper(ICommandExecutor commandExecutor, object command)
        {
            commandExecutor.Execute(command);
            _commandIsPending = false;
            OnCommandComplete?.Invoke();
        }
    }
}