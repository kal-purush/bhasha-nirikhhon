require('dotenv').config();
const NetlifyAPI = require('netlify');
const process = require('process');
const prompt = require('prompt');
const clear = require('clear');
const util = require('util');
const Handlebars = require('handlebars');
const fs = require('fs');

const template = Handlebars.compile(
  fs.readFileSync('./comment-template.md', 'utf8')
);
const siteId = process.env.NETLIFY_SITE_ID;
const token = process.env.NETLIFY_TOKEN;
const commentPath = `${__dirname}/content/comments`;

async function main() {
  const client = new NetlifyAPI(token);
  const formSubmissions = await client.listSiteSubmissions({ site_id: siteId });
  if (formSubmissions.length === 0) {
    console.log('No new comments available');
    process.exit(0);
  }

  const schema = {
    properties: {
      option: {
        pattern: /^[123]{1}$/,
        description: 'Please Choose an Option',
        message: 'Please enter a valid option',
        required: true,
      },
    },
  };
  const continueSchema = {
    properties: {
      continue: {
        description: 'Press any key to continue',
      },
    },
  };
  prompt.start();
  for (const submission of formSubmissions) {
    clear();
    try {
      console.log('Current Comment Submission');
      console.log('================');
      console.log(`Name: ${submission.data.name}`);
      console.log(`Page: ${submission.data.slug}`);
      console.log(`Date: ${submission.created_at}`);
      console.log(`Gdpr: ${submission.data.gdpr}`);
      console.log('Comment:');
      console.log(submission.data.comment);
      console.log('');
      console.log('----------------');
      console.log('Select an Option');
      console.log('1. Skip comment');
      console.log('2. Approve comment');
      console.log('3. Remove comment (irreversible)');

      const result = await util.promisify(prompt.get)(schema);
      const value = Number.parseInt(result.option, 10);
      switch (value) {
        case 1:
          console.log('Current comment was skipped');
          break;
        case 2:
          if (!submission.data.gdpr) {
            console.log('Invalid GDPR flag');
            break;
          }
          const contents = template({
            name: submission.data.name,
            slug: submission.data.slug,
            date: submission.created_at,
            comment: submission.data.comment,
          });
          const directory = `${commentPath}${submission.data.slug}`;
          if (!fs.existsSync(directory)) {
            await util.promisify(fs.mkdir)(directory, { recursive: true });
          }
          const fileName = `entry-${submission.id}.md`;
          const path = `${directory}${fileName}`;
          await util.promisify(fs.writeFile)(path, contents);
          console.log(`Comment successfully saved in: ${path}`);
          break;
        case 3:
          await client.deleteSubmission({ submission_id: submission.id });
          console.log('Comment successfully deleted');
          break;
        default:
          console.log('default');
      }
    } catch (error) {
      console.error(error);
    }
    await util.promisify(prompt.get)(continueSchema);
  }

  console.log('');
  console.log('No more comments available');
  process.exit(0);
}

main();