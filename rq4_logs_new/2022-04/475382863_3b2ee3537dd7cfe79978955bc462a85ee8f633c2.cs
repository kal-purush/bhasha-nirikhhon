namespace CleanArchitecture_Example.Scripts.Domain.UseCases
{
    public class ShowLoadingScreenUseCase : IShowLoadingScreenUseCase
    {
        private readonly IShowLoadingUseCaseOutput _output;

        public ShowLoadingScreenUseCase(IShowLoadingUseCaseOutput output)
        {
            _output = output;
        }
        
        public void Show()
        {
            _output.SetOutput(true);
        }

        public void Hide()
        {
            _output.SetOutput(false);
        }
    }
}