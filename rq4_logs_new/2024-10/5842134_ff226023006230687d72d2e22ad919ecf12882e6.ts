import tl = require('azure-pipelines-task-lib/task');
import 'dotenv/config';
 async function run() {
     try {
         const inputString: string | undefined = tl.getInput('samplestring', true);
         if (inputString == 'bad') {
             tl.setResult(tl.TaskResult.Failed, 'Bad input was given');
             return;
         }
         console.log('Hello', inputString);
     }
     catch (err:any) {
         tl.setResult(tl.TaskResult.Failed, err.message);
     }
 }

 run();