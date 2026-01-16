
module.exports = {

    /**
     * @name getCustomer
     * @description Simple Customer Sample Extraction In Json 
     */
    getCustomers: function (req, res, next) {

        var result = {doc: [
                        { customerName: 'Jonathan Lister', linkedInUrl: 'https://uk.linkedin.com/in/jayfresh' },
                        { customerName: 'Amy Elizabeth', linkedInUrl: 'https://uk.linkedin.com/pub/amy-elizabeth/26/346/393' },
                        { customerName: 'Tim Westrgren', linkedInUrl: 'https://www.linkedin.com/pub/tim-westergren/18/b77/459' },   
                        { customerName: 'Arianna Huffington', linkedInUrl: 'https://www.linkedin.com/pub/arianna-huffington/40/158/aa7' },
                        { customerName: 'William H. Gates', linkedInUrl: 'https://www.linkedin.com/in/williamhgates' },
                        { customerName: 'Barack Obama', linkedInUrl: 'https://www.linkedin.com/in/barackobama' },
                        { customerName: 'Sean Parker', linkedInUrl: 'https://www.linkedin.com/pub/sean-parker/0/1/826' },
                        { customerName: 'Jonny Lis', linkedInUrl: 'https://ph.linkedin.com/pub/jonny-lis/27/ab5/43a' },
                        { customerName: 'Reid Hoffman', linkedInUrl: 'https://www.linkedin.com/in/reidhoffman'}
                        
                    ]};
        res.send(result);
    }

};



