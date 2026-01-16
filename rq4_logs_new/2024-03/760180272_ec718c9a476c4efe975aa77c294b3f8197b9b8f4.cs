// OPERADORES
int a = 3;
int b = a ++; //++ é um acumulador
// Neste ++ vai somar a + 1
// Entretanto o acúmulo vai ocorrer depois da
// atribuição pois o ++ está do lado direito de a
Console.WriteLine($"a é {a}, b é {b}");

//-------------------------
int c = 3;
int d = ++c;
Console.WriteLine($"c é {c}, d é {d}");
// Neste caso o ++ está do lado esquerdo de c,
// Portado o acúmulo ocorre antes da atribuição
//----------------
//Combinando operadores de atribuição
int p = 6;
p += 3; //Equivalente a p = p + 3;
p -= 3; //Equivalente a p = p - 3;
p *= 3; //Equivalente a p = p * 3;
p /= 3; //Equivalente a p = p / 3;

// OPERADORES LÓGICOS

bool b_A = true;
bool b_B = false;
Console.WriteLine($"AND | b_A | b_B ");
Console.WriteLine($"b_A | {b_A & b_A, -5} | {b_A & b_B, -5}");
Console.WriteLine($"b_A | {b_B & b_A, -5} | {b_A & b_B, -5}");
Console.WriteLine();
Console.WriteLine($"OR | b_A | b_B ");
Console.WriteLine($"b_A | {b_A | b_A, -5} | {b_A | b_B, -5}");
Console.WriteLine($"b_A | {b_B | b_A, -5} | {b_A | b_B, -5}");
Console.WriteLine();
Console.WriteLine($"XOR | b_A | b_B ");
Console.WriteLine($"b_A | {b_A ^ b_A, -5} | {b_A ^ b_B, -5}");
Console.WriteLine($"b_A | {b_B ^ b_A, -5} | {b_A ^ b_B, -5}");
Console.WriteLine();