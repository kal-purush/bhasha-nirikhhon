namespace WebApiTemplate.Services.Infrastructure.MessageQueue
{
	public interface IMessageService
	{
		Task SendMessage<T>(T obj)
		where T : class;
	}
}