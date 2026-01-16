import { Server, Socket } from 'socket.io';
import { blockChainService as blockChain } from './block-chain';

const initSocketEvents = (io: Server) => {
  return (socket: Socket) => {
    socket.emit('message', `${(socket as any).user}`);

    socket.on('error', (error) => {
      io.emit('error', `socket error:  ${error}`);
    });

    socket.on(
      'subscribe',
      async (data: { event: string; address: string }, callback) => {
        const { event, address } = data;

        // utilise Job as oppose to setTimeout
        const interval = setInterval(async () => {
          try {
            const { result } = await blockChain.getLatestBlockNumber();

            const transactions = await blockChain.getBlockTransactions(result);

            const response = blockChain.filterCondition(
              transactions,
              address,
              event,
            );

            socket.emit('block', response);
            callback();
          } catch (error) {
            console.error(`Error fetching block data: ${error}`);
            callback(error);
          }
        }, 12 * 1000);

        socket.on('disconnect', () => {
          clearInterval(interval);
        });
      },
    );
  };
};

export { initSocketEvents };