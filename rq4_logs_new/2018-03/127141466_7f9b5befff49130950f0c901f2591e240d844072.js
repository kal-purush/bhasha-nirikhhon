'use strict';


// класс который обрабатывает (хранит форму, трансформирует, и т.д.) текущую падающую фигуру
function Figures (color, type) {
  // основные данные фигуры
  this.type = type;
  this.width = -1;
  this.height = -1;
  // координаты фигуры на игоровом поле gameField
  this.X = 4;
  this.Y = 0; // чтобы появлялась в координате 0
  // предидущие координаты фигуры
  this.PreX = -1;
  this.PreY = -1;
  // определяет столкнется объект если опуститься на 1 единицу вниз
  this.collideDownVar = false;
  // переводим цвет из строкового в числовое внутриигровое значение
  if (typeof(color) == "number") {
    this.color = color;
  } else {
    switch (color)
    {
      case "gray":
        this.color = 0;
      break;
      case "green":
        this.color = 1;
      break;
      case "purple":
        this.color = 2;
      break;
      case "blue":
        this.color = 3;
      break;
      case "yellow":
        this.color = 4;
      break;
      case "turquoise":
        this.color = 5;
      break;
      case "red":
        this.color = 6;
      break;
      default:
        this.color = 99;
      break;
    }
  }
  // определяет форму фигуры
  this.figureForm  = new Array();
  // заполняем массив figureForm в зависимости от типа фигуры
  // для каждой фигуры свой алгоритм генерации
  switch (type) {
    case "I":
      this.width = 1;
      this.height = 4;
      for (var i = 0; i < 4; i++) {
        this.figureForm[i] = new Array();
        this.figureForm[i][0] = 1;
      }
    break;
    case "O":
      this.width = 2;
      this.height = 2;
      this.figureForm[0] = new Array();
      this.figureForm[0][0] = 1;
      this.figureForm[0][1] = 1;
      this.figureForm[1] = new Array();
      this.figureForm[1][0] = 1;
      this.figureForm[1][1] = 1;
    break;
    case "L":
      this.width = 2;
      this.height = 3;
      this.figureForm[0] = new Array();
      this.figureForm[0][0] = 1;
      this.figureForm[0][1] = 0;
      this.figureForm[1] = new Array();
      this.figureForm[1][0] = 1;
      this.figureForm[1][1] = 0;
      this.figureForm[2] = new Array();
      this.figureForm[2][0] = 1;
      this.figureForm[2][1] = 1;
   break;
   case "J":
      this.width = 2;
      this.height = 3;
      this.figureForm[0] = new Array();
      this.figureForm[0][0] = 0;
      this.figureForm[0][1] = 1;
      this.figureForm[1] = new Array();
      this.figureForm[1][0] = 0;
      this.figureForm[1][1] = 1;
      this.figureForm[2] = new Array();
      this.figureForm[2][0] = 1;
      this.figureForm[2][1] = 1;
    break;
    case "S":
      this.width = 3;
      this.height = 2;
      this.figureForm[0] = new Array();
      this.figureForm[0][0] = 0;
      this.figureForm[0][1] = 1;
      this.figureForm[0][2] = 1;
      this.figureForm[1] = new Array();
      this.figureForm[1][0] = 1;
      this.figureForm[1][1] = 1;
      this.figureForm[1][2] = 0;
    break;
    case "Z":
      this.width = 3;
      this.height = 2;
      this.figureForm[0] = new Array();
      this.figureForm[0][0] = 1;
      this.figureForm[0][1] = 1;
      this.figureForm[0][2] = 0;
      this.figureForm[1] = new Array();
      this.figureForm[1][0] = 0;
      this.figureForm[1][1] = 1;
      this.figureForm[1][2] = 1;
    break;
    case "T":
      this.width = 3;
      this.height = 2;
      this.figureForm[0] = new Array();
      this.figureForm[0][0] = 0;
      this.figureForm[0][1] = 1;
      this.figureForm[0][2] = 0;
      this.figureForm[1] = new Array();
      this.figureForm[1][0] = 1;
      this.figureForm[1][1] = 1;
      this.figureForm[1][2] = 1;
    break;
  }
}

// сбрасывает фигуру на первоначальное состояние, когда фигура не может двигаться дальше вниз
Figures.prototype.reset = function() {
  this.X = 4;
  this.Y = 0;
  this.PreX = -1;
  this.PreY = -1;
  this.collideDownVar = false;
}
// рендерит текущее положение фигуры на игровом поле
Figures.prototype.render = function() {
  for (var j = 0; j < this.width; j++)
    for (var i = 0; i < this.height; i++)
    {
      if (this.figureForm[i][j]) {
        gameField[j+this.X][i+this.Y] = this.color;
      }
    }
}
// очищает текущее положение фигуры на поле (удаляет фигуру)
Figures.prototype.clear = function(x, y) {
  
  for (var j = 0; j < this.width; j++)
    for (var i = 0; i < this.height; i++)
    {
      if (this.figureForm[i][j]) {
        gameField[j+this.X][i+this.Y] = 0;
      }
    }
}

// перемещает нашу фигуру вниз
Figures.prototype.moveDown = function() {
  this.clear(); // стираем предыдущую фигуру
  this.Y++; // двигаем фигуру
  this.render(); // отрисовываем фигуру
}

// проверяет сталкивается ли фигура при перемещении вниз или нет
Figures.prototype.collideDown = function() {
  for (var j = 0; j < this.height; j++)
    for (var i = 0; i < this.width; i++) {
      if (j < this.height-1) {
        if (this.figureForm[j][i] && (this.figureForm[j+1][i] == 0)) {
          if (((this.Y+this.height) >= 20) || (gameField[this.X+i][this.Y+j+1] != 0)) {
            return true;
          }
        } 
      } else {
        if (this.figureForm[j][i]) {
          if (((this.Y+this.height) >= 20) || (gameField[this.X+i][this.Y+j+1] != 0)) {
            return true;
          }
        }
      }
    }
  return false;
}


Figures.prototype.moveRight = function() {
  this.clear(); // стираем предыдущую фигуру
  this.X++; // двигаем фигуру
  this.render(); // отрисовываем фигуру
}

Figures.prototype.collideRight = function() {
  for (var j = 0; j < this.height; j++)
  for (var i = 0; i < this.width; i++) {
    if (i < this.width-1) {
      if (this.figureForm[j][i] && (this.figureForm[j][i+1] == 0)) {
        if (((this.X+this.width) >= 10) || (gameField[this.X+i+1][this.Y+j] != 0)) {
          return true;
        }
      } 
    } else {
      if (this.figureForm[j][i]) {
        if (((this.X+this.width) >= 10) || (gameField[this.X+i+1][this.Y+j] != 0)) {
          return true;
        }
      }
    }
  }
  return false;
}

Figures.prototype.moveLeft = function() {
  this.clear(); // стираем предыдущую фигуру
  this.X--; // двигаем фигуру
  this.render(); // отрисовываем фигуру
}

Figures.prototype.collideLeft = function() {
  for (var j = 0; j < this.height; j++)
  for (var i = 0; i < this.width; i++) {
    if (i > 0) {
      if (this.figureForm[j][i] && (this.figureForm[j][i-1] == 0)) {
        if (((this.X) <= 0) || (gameField[this.X-1][this.Y+j] != 0)) {
          return true;
        }
      } 
    } else {
      if (this.figureForm[j][i]) {
        if (((this.X) <= 0) || (gameField[this.X-1][this.Y+j] != 0)) {
          return true;
        }
      }
    }
  }
  return false;
}

// разворот фигуры
Figures.prototype.rotation = function() {
  // преобразовываем наш массив к квадратной матрице, 
  // так как универсального алгоритма для разворота прямоугольной матрицы я не нашёл
  var n;
  if ( this.width >= this.height )
    n = this.width;
  else 
    n = this.height;
  this.clear();
  var tempMatrix = array2xCopy(this.height, this.width, n, this.figureForm);
  var tmp;
  for (var i = 0; i < n/2; i++) {
    for (var j = i; j < n-1-i; j++) {
      tmp = tempMatrix[i][j];
      tempMatrix[i][j] = tempMatrix[n-j-1][i];
      tempMatrix[n-j-1][i] = tempMatrix[n-i-1][n-j-1];
      tempMatrix[n-i-1][n-j-1] = tempMatrix[j][n-i-1];
      tempMatrix[j][n-i-1] = tmp;
    }
  }
  // вычленяем нулевые строки из нашего массива
  // горизонтальные
  var isEmpty = true;
  var m = n;
  for (var i = 0; i < m; i++) {
    isEmpty = true;
    for (var j = 0; j < n; j++)
      if (tempMatrix[i][j])  isEmpty = false;
    
    if (isEmpty) {
        tempMatrix[i].splice(0, m);
        tempMatrix.splice(i, 1);
        m--;
        i--;
    }
  }
  // и вертикальные
  for (var i = 0; i < n; i++) {
    isEmpty = true;
    for (var j = 0; j < m; j++) {
      if (tempMatrix[j][i])  isEmpty = false;
    }
    if (isEmpty) {
        for (var j = 0; j < m; j++)
        {
          tempMatrix[j].splice(i, 1);
        }
        n--;
        i--;
    }
  }

  if (this.collideRotation(tempMatrix, this.height, this.width)) {
    // уничтожаем старый массив
    for (i = 0; i < this.height; i++)
    {
      this.figureForm[i].splice(0, this.width-1);
    }
  
    // создаем новую размерность с новым массивом 
    var tmp = this.width; 
    this.width = this.height;
    this.height = tmp;
    this.figureForm = matrixGen (this.height, this.width, 0);
    this.figureforn = new Array(this.height);
    for (var i = 0; i < this.height; i++) {
      this.figureForm[i] = new Array(this.width);
      for (var j = 0; j < this.width; j++) {
        this.figureForm[i][j] = tempMatrix[i][j];
      }
    }
  }

  this.render();
}

// 
Figures.prototype.collideRotation = function(chekedFigure, width, height) {
  if ((this.X + width) > 10) return false;
  if (this.Y + height > 20) return false;

  for ( var i = 0; i < height; i++) {
    for ( var j =0; j < width; j++) {
      if (chekedFigure[i][j]) 
        if (gameField[this.X+j][this.Y+i] != 0)
          return false;
    }
  }
  
  if (curGameStatus.value == gameStatuses.figuraMove.value)
    return true;
  else return false;
}

// не относится к классу Figures
// функция выбирает произвольную фигуру
function randFigureGen() {
  var color = gRand (0, 4);
  var figureType = gRand (0, 6);
  //console.log("color = " + color);
  //console.log("figureType = " + figureType);
  switch (figureType)
  {
    case 0:
      switch (color) 
      {
        case 0: return figureI_green; break;
        case 1: return figureI_purple; break;
        case 2: return figureI_yellow; break;
        case 3: return figureI_turquoise; break;
        case 4: return figureI_red; break;
        default: console.log("Ошибка: Фигура типа I недопустимый аргумент в выборе цвета"); break;
      }
    break;
    case 1:
      switch (color) 
      {
        case 0: return figureO_green; break;
        case 1: return figureO_purple; break;
        case 2: return figureO_yellow; break;
        case 3: return figureO_turquoise; break;
        case 4: return figureO_red; break;
        default: console.log("Ошибка: Фигура типа O недопустимый аргумент в выборе цвета"); break;
      }
    break;
    case 2:
      switch (color) 
      {
        case 0: return figureL_green; break;
        case 1: return figureL_purple; break;
        case 2: return figureL_yellow; break;
        case 3: return figureL_turquoise; break;
        case 4: return figureL_red; break;
        default: console.log("Ошибка: Фигура типа L недопустимый аргумент в выборе цвета"); break;
      }
    break;
    case 3:
      switch (color) 
      {
        case 0: return figureJ_green; break;
        case 1: return figureJ_purple; break;
        case 2: return figureJ_yellow; break;
        case 3: return figureJ_turquoise; break;
        case 4: return figureJ_red; break;
        default: console.log("Ошибка: Фигура типа J недопустимый аргумент в выборе цвета"); break;
      }
    break;
    case 4:
      switch (color) 
      {
        case 0: return figureS_green; break;
        case 1: return figureS_purple; break;
        case 2: return figureS_yellow; break;
        case 3: return figureS_turquoise; break;
        case 4: return figureS_red; break;
        default: console.log("Ошибка: Фигура типа S недопустимый аргумент в выборе цвета"); break;
      }
    break;
    case 5:
      switch (color) 
      {
        case 0: return figureZ_green; break;
        case 1: return figureZ_purple; break;
        case 2: return figureZ_yellow; break;
        case 3: return figureZ_turquoise; break;
        case 4: return figureZ_red; break;
        default: console.log("Ошибка: Фигура типа Z недопустимый аргумент в выборе цвета"); break;
      }
    break;
    case 6:
      switch (color) 
      {
        case 0: return figureT_green; break;
        case 1: return figureT_purple; break;
        case 2: return figureT_yellow; break;
        case 3: return figureT_turquoise; break;
        case 4: return figureT_red; break;
        default: console.log("Ошибка: Фигура типа T недопустимый аргумент в выборе цвета"); break;
      }
    break;
    default: console.log("Ошибка: Недопустимый тип фигуры"); break;
  }
}






