//create action method show
exports.show = (req, res) => {

    // https://localhost:4000/ || https://localhost:4000/about  || /contact
    const path = (req.path === `/`) ? `/home` : req.path;

    // render the view
    res.render(`pages${path}`);         //generate views/pages/about
};