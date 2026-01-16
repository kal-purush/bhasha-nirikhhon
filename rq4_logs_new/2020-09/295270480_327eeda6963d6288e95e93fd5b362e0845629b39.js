let MAX_CHOICES = 4;
let quizes = [];
quizes.push(content_quiz1());
quizes.push(content_quiz2());
quizes.push(content_quiz3());

function content_quiz1() {
  let info = { project: "Spanish", section: "Vocabulary", topic: "Primary Colors", header: "Colors" };
  let options = { random: true, count: [8] };
  let quiz = build_choices([
    ['negro', 'Black'],
    ['rojo',  'Red'],
    ['verde', 'Green'],
    ['azul',  'Blue'],
    ['amarillo',  'Yellow'],
    ['blanco', 'White'],
    ['gris',  'Gray'],
    ['naranja', 'Orange'],
    ['lila', 'Purple'],
    ['rosa', 'Pink'],
    ['marrón', 'Brown'],
    ['Firozi', 'Turquoise'],
    ['claro', 'Light'],
    ['oscuro', 'Dark'],
  ], options);
  return {info, quiz};
}

function content_quiz2() {
  let info = { project: "Spanish", section: "Vocabulary", topic: "Counting", header: "Counting" };
  let options = { random: true, count: [12] };
  let quiz = build_choices([
    ['Uno', 'One'],
    ['Dos', 'Two'],
    ['Tres', 'Three'],
    ['Quatro', 'Four'],
    ['Cinco',  'Five'],
    ['Seis',  'Six'],
    ['Siete',  'Seven'],
    ['ocho', 'Eight'],
    ['nueve',   'Nine'],
    ['diez',  'Ten'],
    ['once',  'Eleven'],
    ['doce',  'Twelve'],
    ['trece',   'Thirteen'],
    ['catorce', 'Fourteen'],
    ['quince', 'Fifteen'],
    ['dieciséis',   'Sixteen'],
    ['diecisiete', 'Seventeen'],
    ['dieciocho', 'Eighteen'],
    ['diecinueve',  'Nineteen'],
    ['veinte',  'Twenty'],
    ['veintiuno',  'Twenty-one'],
    ['veintidós',  'Twenty-two'],
    ['veintitrés',  'Twenty-three'],
    ['veinticuatro', 'Twenty-four'],
    ['veinticinco', 'Twenty-five'],
  ], options);
  return {info, quiz};
}

function content_quiz3() {
  let info = { project: "Spanish", section: "Vocabulary", topic: "Vegitables", header: "Vegitables" };
  let options = { random: true, count: [11] };
  let quiz = build_choices([
    ['patata', 'Potato'],
    ['coliflor',  'Cauliflower'],
    ['guisante', 'Peas'],
    ['zanahoria', 'Carrot'],
    ['rábano', 'Radish'],
    ['berenjena', 'Eggplant'],
    ['quingombó', 'Okra'],
    ['maíz',    'Corn'],
    ['lenteja', 'Lentil'],
    ['patata', 'Spinach'],
    ['tomate', 'Tomato'],
    ['jengibre', 'Ginger'],
    ['ajo', 'Garlic'],
    ['encurtir', 'Pickle'],
  ], options);
  return {info, quiz};
}

function content_quiz4() {
  let info = { project: "Spanish", section: "Vocabulary", topic: "Timings, Past Present and Future", header: "Timings" };
  let options = { random: true, count: [8] };
  let quiz = build_choices([
    ['Hoy', 'Today'],
    ['Mañana', 'Tomorrow'],
    ['Ayer', 'Yesterday'],
    ['Pasado mañana', 'Day after tommorrow'],
    ['Anteayer', 'Day before yesterday'],
    ['Es Temprano', 'Is early'],
    ['Es Tarde', 'Is late'],
    ['Dia', 'Day'],
    ['Noche', 'Night'],
    ['Mañana', 'Morning'],
    ['Noche',  'Evening'],
    ['Tarde', 'Afternoon'],
    ['Mediodia', 'Mid-day'],
    ['Medianoche', 'Mid-night'],
  ], options);
  return {info, quiz};
}

function build_choices(data, options) {
  if(options.count) {
    data = data.slice(0, options.count[0]);
  }
  let choices = [];
  for(let row of data) {
    choices.push(row[1])
  }
  for(let row of data) {
    this_choices = choices.slice(0);
    remove_item(row[1], this_choices);
    this_choices = shuffle(this_choices);
    this_choices.unshift(row[1]);
    this_choices = this_choices.slice(0, MAX_CHOICES);
    this_choices = shuffle(this_choices);
    row[2] = this_choices.slice(0, MAX_CHOICES);
  }
  if(options.random) data = shuffle(data);
  return data;
}
function remove_item(item, list) {
  let index = list.indexOf(item);
  if(index < 0) return list;
  list.splice(index, 1);
  return list;
}
function shuffle(array) {
  return array.sort(() => Math.random() - 0.5);
}