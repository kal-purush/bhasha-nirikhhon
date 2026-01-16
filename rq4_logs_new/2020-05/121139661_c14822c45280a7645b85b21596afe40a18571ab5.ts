import path from 'path';
import { createFilePath } from 'gatsby-source-filesystem';
import parseArticlePath from '../utils/parseArticlePath';

const query = `
    {
        allMarkdownRemark(filter: { frontmatter: { publish: { ne:false } } }) {
            edges {
                node {
                    fields {
                        slug
                    }
                }
            }
        }
    }
`;

/**
 * onCreateNode
 */
export function onCreateNode({ node, getNode, actions }) {
    const { createNodeField } = actions;
    if (node.internal.type === 'MarkdownRemark') {
        const relativeFilePath = createFilePath({
            node,
            getNode,
            basePath: 'articles',
        });

        createNodeField({
            node,
            name: 'slug',
            value: `/articles${relativeFilePath}`,
        });

        const [
            date,
            index,
            name,
        ] = parseArticlePath(relativeFilePath);

        createNodeField({ node, name: 'date', value: date });
        createNodeField({ node, name: 'index', value: index });
        createNodeField({ node, name: 'name', value: name });
    }
}

export function createPages({ graphql, actions }) {
    const { createPage } = actions;

    return new Promise((resolve, reject) => {
        graphql(query).then((result) => {
            result.data.allMarkdownRemark.edges.forEach(({ node }) => {
                createPage({
                    path: node.fields.slug,
                    component: path.resolve('./src/templates/ArticlePage.jsx'),
                    context: {
                        slug: node.fields.slug,
                    },
                });
            });
            resolve();
        }).catch((error: Error) => {
            reject(error);
        });
    });
}