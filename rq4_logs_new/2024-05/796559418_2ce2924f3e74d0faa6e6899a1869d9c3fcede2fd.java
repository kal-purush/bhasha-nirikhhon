public abstract class Menu {

    /*Metodos*/
    public static final String RESET = "\033[0m";
    public static final String ANSI_BLUE_LIGHT = "\033[94m";  // Azul claro
    public static final String ANSI_BLUE_DARK = "\033[34m";    // Azul oscuro
    public static final String ANSI_GRAY = "\033[90m";        // Gris
    public static final String ANSI_WHITE = "\033[97m";        // Blanco

    public static void titulo() {
        String 	titulo = ANSI_BLUE_DARK +
                "\t\t\t\t\t\t\t\t\t\t\t   ████████ ██████  ██  ██████  ██████  ██    ██ ███    ██ ████████ \n" +
                "\t\t\t\t\t\t\t\t\t\t\t      ██    ██   ██ ██ ██      ██    ██ ██    ██ ████   ██    ██    \n" +
                "\t\t\t\t\t\t\t\t\t\t\t      ██    ██████  ██ ██      ██    ██ ██    ██ ██ ██  ██    ██    \n" +
                "\t\t\t\t\t\t\t\t\t\t\t      ██    ██   ██ ██ ██      ██    ██ ██    ██ ██  ██ ██    ██    \n" +
                "\t\t\t\t\t\t\t\t\t\t\t      ██    ██   ██ ██  ██████  ██████   ██████  ██   ████    ██    \n" +
                "\t\t\t\t\t\t\t\t\t\t\t                                                                    \n" +
                "                                                                 ";
        System.out.print(titulo);
    }


        public abstract void tituloSecundario();

    public abstract void subtitulo();
    public abstract void subtitulo2();
    public abstract void mensajeInicio();
    public abstract void mensajeFin();
    public static void creditos(){
        System.out.println(ANSI_BLUE_LIGHT+"\t\t\t\t ʙʏ ᴀɴᴅʀᴇꜱ ᴅᴀɴɪᴇʟ ᴍɪᴋᴇʟ ʏᴀꜱɪʀ"+"\n\n");
    }
}