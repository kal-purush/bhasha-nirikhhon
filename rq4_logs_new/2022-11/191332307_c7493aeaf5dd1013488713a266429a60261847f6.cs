using System;

namespace Core.Serialization.JSON
{
	public static partial class
									JSON<T>
	{
		static
									JSON
										(
										)
		{
            Deserialize = Deserialize_System_Text_Json<T>;

            return;
		}

        public static
            Func<string, T>
                                    Deserialize;


        public static
            T
									Deserialize_System_Text_Json<T>
										(
                                            string json
                                        )
        {
            return System.Text.Json.JsonSerializer.Deserialize<T>(json);
        }

        public static
            T
                                    Deserialize_Newtonsoft_Json<T>
                                        (
                                            string json
                                        )
        {
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
        }

        public static
            T
                                    Deserialize_Jil<T>
                                        (
                                            string json
                                        )
        {
            return Jil.JSON.Deserialize<T>(json);
        }
    }
}
