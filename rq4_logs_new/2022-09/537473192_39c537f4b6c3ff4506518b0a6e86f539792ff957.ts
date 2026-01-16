import Dockerode from 'dockerode';

const HumanReadableContainer = async (host: Dockerode, id: string) => {
  const container = host.getContainer(id);

  const containerStats = await container.stats();
  const containerData = await container.inspect();

  return {
    ...containerData,
  };
};

export default HumanReadableContainer;