import path from 'path';
import pact from '@pact-foundation/pact-node';

const opts = {
  pactUrls: [path.resolve(process.cwd(), 'pacts')],
  pactBroker: 'http://localhost:9292',
  consumerVersion: '1.0.0'
};

pact.publishPacts(opts)
  .then(() => {
    console.log('Pact contract publishing complete!');
    console.log('');
    console.log('Head over to http://localhost:9292');
    console.log('to see your published contracts.');
    process.exit(0);
  })
  .catch(e => {
    console.error('Pact contract publishing failed: ', e);
    process.exit(1);
  });