function setup() {
    const data = tf.tensor([0, 0, 127,45324,345,453,5334,35], [2, 2, 2]); //creo un tensor che ha 2 matrici da 2x2 (2x2x2=8 numeri)
    data.print();
}
setup();