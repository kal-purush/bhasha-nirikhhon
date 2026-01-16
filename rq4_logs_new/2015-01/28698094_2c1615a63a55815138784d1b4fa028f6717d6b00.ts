function random(length) {
    return ~~(Math.random() * length);
}

function Genetic(population, startLength, alphabet, isFixedLength) {
    if (typeof isFixedLength === 'undefined')
        isFixedLength = true;

    this.population = population;
    this.startLength = startLength;
    this.alphabet = alphabet;
    this.isFixedLength = isFixedLength;
    this.elitism = 0.4; // percentage of population kept from the last generation
    this.mutation = 0.01; // chance of mutation
    this.mating = 0.5; // size of the population needed for mating
    this.genes = [];

    this.fill(this.population);
    this.isSorted = false;
}

Genetic.prototype.fill = function(count) {
    while (this.genes.length < this.population && count--) {
        var example = '';
        for (var i = 0; i < this.startLength; ++i)
            example += this.alphabet[random(this.alphabet.length)];

        this.genes.push(example);
    }
}

// create children by taking half of one parent, half of another.  
// keep mating until the population is full
Genetic.prototype.mate = function() {
    var genes = this.genes;
    var matingSize = this.population * this.mating;

    this.fill(matingSize - genes.length);

    while (genes.length < this.population) {
        var a = genes[random(genes.length)]; //matingSize)]; //genes.length)];
        var b = genes[random(genes.length)]; //matingSize)]; //genes.length)];
        var splitA = random(a.length);
        var splitB = this.isFixedLength ? splitA : random(b.length);

        var gene = a.substr(0, splitA) + b.substr(splitB, b.length);
        genes.push(this.mutate(gene));
    }
}

Genetic.prototype.kill = function() {
    this.genes.length = ~~(this.elitism * this.genes.length);
}

Genetic.prototype.sort = function(scoreFunc) {
    this.genes.sort(function(a, b) {
        return scoreFunc(a) - scoreFunc(b)
    });
}

// attempt to mutate each element of each gene
Genetic.prototype.mutate = function(gene) {
    for (var j = 0; j < gene.length; ++j) {
        if (Math.random() < this.mutation) {
            var c = this.alphabet[random(this.alphabet.length)];
            gene = gene.substr(0, j) + c + gene.substr(j + 1);
        }
    }
    return gene;
}

Genetic.prototype.generate = function(scoreFunc) {
    if (!this.isSorted) {
        this.sort(scoreFunc);
        this.isSorted = true;
    }

    this.kill();
    this.mate();
    // this.mutate();
    this.sort(scoreFunc);

    return scoreFunc(this.genes[0]);
}