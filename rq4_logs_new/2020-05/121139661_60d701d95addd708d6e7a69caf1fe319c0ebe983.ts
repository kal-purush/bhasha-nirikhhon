import path from 'path';
import { GatsbyNode } from 'gatsby';
import { MarkdownRemarkConnection } from '../../types/graphqlTypes';

const query = `
    query PagesQuery {
        allMarkdownRemark(filter: { frontmatter: { publish: { ne: false } } }) {
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

type Result = {
    allMarkdownRemark: MarkdownRemarkConnection,
};

/**
 * createPages
 */
const createPages: GatsbyNode['createPages'] = async ({ graphql, actions }) => {
    const { createPage } = actions;

    const result = await graphql<Result>(query);
    if (result.errors) throw result.errors;

    const nodes = result?.data?.allMarkdownRemark.edges;
    if (nodes == null) return;

    nodes.forEach(({ node }) => {
        const nodePath = node?.fields?.slug;
        if (nodePath == null) throw new Error('node.field.slug should not be nullish');

        createPage({
            path: nodePath,
            component: path.resolve('./src/templates/ArticlePage.tsx'),
            context: {
                slug: nodePath,
            },
        });
    });
};

export default createPages;