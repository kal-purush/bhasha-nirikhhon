namespace ParticipationRoulette
{
    class Program
    {
        static void Main(string[] args)
        {
            bool showMenu = true;
            while (showMenu)
            {
                showMenu = MainMenu();
            }
        }
        private static bool MainMenu()
        {
            Console.Clear();
            Console.WriteLine("Choose an option:");
            Console.WriteLine("1) Choose Student to Answer Question");
            Console.WriteLine("2) Get Random Groups Composition");
            Console.WriteLine("3) Exit");
            Console.Write("\r\nSelect an option: ");
 
            switch (Console.ReadLine())
            {
                case "1":
                    StudentToAnswer();
                    return true;
                case "2":
                    GroupsComposition();
                    return true;
                case "3":
                    return false;
                default:
                    return true;
            }
        }
 
        private static int CaptureInput()
        {
            Console.Write("How many people per group?");
            return Convert.ToInt32(Console.ReadLine());
        }
 
        private static void StudentToAnswer()
        {
            var students = new List<string>(){};
            students.Add("AFONSO Almeida");
            students.Add("PAULO Henriques");
            students.Add("NUNO Pereira");
            students.Add("DANIEL Nogueira");
            students.Add("Pedro MARQUES");
            students.Add("BRUNO Moreira");
            students.Add("PEDRO MONTEIRO da Rocha");
            students.Add("João Costa");
            students.Add("LUÍS Moreira");
            students.Add("BRUNO LOPES");
            students.Add("MATHEUS Silva");
            students.Add("CRISTIANO Loução");
            students.Add("DIOGO SABINO");
            students.Add("Gonçalo Freitas");
            students.Add("BRUNO MAGALHÃES");
            students.Add("GONÇALO PEREIRA");
            students.Add("NUNO FERRAZ");
            students.Add("DIOGO SILVA");
            students.Add("SIMONE Santos");
            students.Add("RAFAEL Silva");
            students.Add("PEDRO MARTINS");
            students.Add("PEDRO NEVES");
            students.Add("TIAGO Pinto");
            students.Add("GUSTAVO Ribeiro");
            students.Add("JOÃO Barros");
            students.Add("PEDRO COSTA");
            students.Add("MARCOS Ribeiro");
            students.Add("LEANDRO Luz");
            students.Add("FRANCISCO DIAS");
            students.Add("LEONARDO Andrade");

            //Get Random Value to find student
            //Este é outro comentário que me tinha esquecido. 
            var random = new Random(DateTime.Now.Millisecond);
            var  index = random.Next(students.Count);
            
            Console.WriteLine(students[index]);
            Console.Write("\r\nPress Enter to return to Main Menu");
            Console.ReadLine();
        }
 
        private static void GroupsComposition()
        {
            var students = new List<string>(){};
            students.Add("AFONSO Almeida");
            students.Add("PAULO Henriques");
            students.Add("NUNO Pereira");
            students.Add("DANIEL Nogueira");
            students.Add("Pedro MARQUES");
            students.Add("BRUNO Moreira");
            students.Add("PEDRO MONTEIRO da Rocha");
            students.Add("João Costa");
            students.Add("LUÍS Moreira");
            students.Add("BRUNO LOPES");
            students.Add("MATHEUS Silva");
            students.Add("CRISTIANO Loução");
            students.Add("DIOGO SABINO");
            students.Add("Gonçalo Freitas");
            students.Add("BRUNO MAGALHÃES");
            students.Add("GONÇALO PEREIRA");
            students.Add("NUNO FERRAZ");
            students.Add("DIOGO SILVA");
            students.Add("SIMONE Santos");
            students.Add("RAFAEL Silva");
            students.Add("PEDRO MARTINS");
            students.Add("PEDRO NEVES");
            students.Add("TIAGO Pinto");
            students.Add("GUSTAVO Ribeiro");
            students.Add("JOÃO Barros");
            students.Add("PEDRO COSTA");
            students.Add("MARCOS Ribeiro");
            students.Add("LEANDRO Luz");
            students.Add("FRANCISCO DIAS");
            students.Add("LEONARDO Andrade");
            
            Console.Clear();

            var membersPerTeam = CaptureInput();

            int teamCount = students.Count / membersPerTeam;
            var teams = new List<String[]>();

            //This for cycle will create as many groups as needed
            for (int i = 0; i < teamCount; i++)
            {
                //This for cycle will create team composition
                String[] team = new String[membersPerTeam];
                for (int j = 0; j < membersPerTeam; j++)
                {
                    var random = new Random(DateTime.Now.Millisecond);
                    var  index = random.Next(students.Count);
                    var teamMember = students[index];
                    team[j] = teamMember;
                    students.Remove(teamMember);
                }
                teams.Add(team);
            }

            for(int i=0;i<teams.Count;i++)
            {
                var team = teams[i];
                var groupNr = i+1;
                Console.WriteLine("###################    Grupo" + groupNr + "    ###################");
                for(int j=0; j < team.Length; j++)
                {
                    Console.WriteLine(team[j]);
                }
            }

            Console.Write("\r\nPress Enter to return to Main Menu");
            Console.ReadLine();
        }
    }
}