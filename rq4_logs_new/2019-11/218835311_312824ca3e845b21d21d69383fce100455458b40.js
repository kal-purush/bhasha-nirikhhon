const core = require('@actions/core');
const github = require('@actions/github');
const graphql = require("@octokit/graphql");

async function archiveItems() {
    try {
        const query = `query GetItems($org: String!, $project: Int!)
        {
            organization(login: $org) {
              project(number: $project) {
                state
                resourcePath
                columns(last: 1) {
                  edges {
                    node {
                      id
                      name
                      cards {
                        edges {
                          node {
                            id
                            updatedAt
                            isArchived
                            resourcePath
                            databaseId
                            content {
                              ... on Issue {
                                title
                                state
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }`;
        
          const updateCard = `
        mutation ArchiveProjectCard($id: ID!) {
            updateProjectCard(input:{clientMutationId: "archive-project-board-action"
              isArchived: true, projectCardId:$id}) {
                projectCard {
                id
                updatedAt
                }
            }
        }`;        

        let org = core.getInput('organization'); // "github";
        let project = parseInt(core.getInput('project')); // 907;
        let token = core.getInput('token');
        let archivePeriodDays = core.getInput('archive-period-days');
    
        let archivedCount = 0;
    
        const graphqlWithAuth = graphql.graphql.defaults({
            headers: {
              authorization: `token ${token}`
            }
          });

        const archivedItems = await graphqlWithAuth({
            query: query,
            org: org,
            project: project
        });

        const cutoff = new Date();
        cutoff.setDate(cutoff.getDate() - archivePeriodDays);
        if (archivedItems.organization.project.columns.edges.length == 1) {
            for (i = 0;i < archivedItems.organization.project.columns.edges[0].node.cards.edges.length; i++) {
                const card = archivedItems.organization.project.columns.edges[0].node.cards.edges[i].node;
                const cardDate = new Date(card.updatedAt);
                if (!card.isArchived && cardDate < cutoff && card.content.state == 'CLOSED') 
                {
                    console.log('Trying to archive card: ' + card.id + ' Title: ' + card.content.title);

                    const result = await graphqlWithAuth({
                        query: updateCard,
                        id: card.id      
                    });

                    const message = `Card: ${result.updateProjectCard.projectCard.id}, title: ${card.content.title}, archived at: ${result.updateProjectCard.projectCard.updatedAt}`; 
                    console.log(message);
                    archivedCount++;

                    break;
                }
            }
        }            
        core.setOutput("archived-items", archivedCount);
    } 
    catch (error) {
        core.setFailed(error.message);
    }
}

archiveItems();