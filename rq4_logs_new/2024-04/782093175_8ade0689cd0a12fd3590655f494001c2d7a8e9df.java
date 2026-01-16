class Main{
        public static void sun() {
            System.out.println("shine!");
        }
        public static void summer( int temp ) {
            if( temp > 80 ){
                System.out.println("It's a sunny day!");
                temp -= 1;
                summer( temp );
            }
        }