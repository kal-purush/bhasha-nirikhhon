
exports.createPages = async ({actions }) => {  // 1 
    const { createPage } = actions; // 2 
    const dataSource = { thirdSlideTitle: '예방 행동 수칙' }; 
    createPage({ // 3 
        path: '/', // 4 
        component: require.resolve('./src/templates/single-page.js'), // 5 
        context: { dataSource }, // 6
     }); 
    };