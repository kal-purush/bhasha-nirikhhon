using StudentAttandanceLibrary.Models;

namespace StudentAttandanceLibrary.CommonFunctions
{
    public class AutoGenerate
    {
        public string GenerateStudentCode(List<Student> list1, List<Student> list2)
        {
            int k = DateTime.Now.Year - 2006 + 1;
            string code = "HE" + k + "0000";
            if (list1.Select(x => x.StudentId.Contains("HE" + k)).Count() + list2.Count() > 9999)
            {
                return null;
            }
            if (list1.Count() == 0 && list2.Count() == 0)
            {
                return code;
            }
            else
            {
                string lastStudentCode = (list2.Count == 0) ? list1.Last().StudentId : list2.Last().StudentId;
                string number = lastStudentCode.Remove(0, 4).ToString();
                int lastNumber = int.Parse(number) + 1;
                if (lastNumber >= 1 && lastNumber <= 9)
                {
                    code = code.Remove(4, 1) + lastNumber;
                }
                if (lastNumber >= 10 && lastNumber <= 99)
                {
                    code = code.Remove(4, 2) + lastNumber;
                }
                if (lastNumber >= 100 && lastNumber <= 999)
                {
                    code = code.Remove(4, 3) + lastNumber;
                }
                if (lastNumber >= 1000 && lastNumber <= 9999)
                {
                    code = code.Remove(4, 4) + lastNumber;
                }
            }
            return code;
        }

        public string GenerateTeacherCode(List<Teacher> list1, List<Teacher> list2, string fullName)
        {
            string displayName = GenerateDisplayName(fullName);
            if (CheckDuplicateDisplayName(list1, displayName) || CheckDuplicateDisplayName(list2, displayName))
            {
                return displayName + GetNumberWhenDuplicateName(list1, list2, displayName);
            }
            return displayName;
        }

        public string GenerateDisplayName(string fullName)
        {
            string[] names = fullName.Split(" ");
            string lastName = names[names.Length - 1].ToString()
                .Replace(names[names.Length - 1][0], char.ToUpper(names[names.Length - 1][0]));
            for (int i = 1; i < lastName.Length; i++)
            {
                lastName.Replace(lastName[i], char.ToLower(lastName[i]));
            }
            string displayName = lastName;
            for (int i = 0; i < names.Length - 1; i++)
            {
                displayName += names[i][0].ToString().ToUpper();
            }
            return displayName;
        }

        public bool CheckDuplicateDisplayName(List<Teacher> list, string displayName)
        {
            for (int i = 0; i < list.Count; i++)
            {
                if (displayName.ToUpper().Equals(GetDuplicateDisplayName(list[i].TeacherId.ToUpper())))
                {
                    return true;
                }
            }
            return false;
        }

        public string GetDuplicateDisplayName(string displayName)
        {
            string userName = "";
            for (int i = 0; i < displayName.Length; i++)
            {
                if (char.IsLetter(displayName[i]))
                {
                    userName += displayName[i];
                }
            }
            return userName;
        }

        public int GetNumberWhenDuplicateName(List<Teacher> list1, List<Teacher> list2, string displayName)
        {
            int count = 0;
            for (int i = 0; i < list1.Count; i++)
            {
                if (displayName.ToUpper().Equals(GetDuplicateDisplayName(list1[i].TeacherId.ToUpper())))
                {
                    count++;
                }
            }
            for (int i = 0; i < list2.Count; i++)
            {
                if (displayName.ToUpper().Equals(GetDuplicateDisplayName(list2[i].TeacherId.ToUpper())))
                {
                    count++;
                }
            }
            return count;
        }
    }
}