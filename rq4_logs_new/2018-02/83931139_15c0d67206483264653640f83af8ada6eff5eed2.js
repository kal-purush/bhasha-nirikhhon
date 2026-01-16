module.exports = {
    siteMetadata: {
        title: 'Igor Barsi | Breaking the web, one coffee-less commit at a time!',
    },
    plugins: [
        'gatsby-plugin-react-helmet',
        {
            resolve: 'gatsby-source-filesystem',
            options: {
                path: `${ __dirname }/src/pages/`,
                name: 'pages',
            },
        },
        'gatsby-transformer-remark',
    ],
};