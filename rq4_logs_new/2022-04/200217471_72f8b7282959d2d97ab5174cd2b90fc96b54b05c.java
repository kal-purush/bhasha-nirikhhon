package top.auzqy.design.exercise.factory.exercise001;
class Engine {
        public void getStyle(){
            System.out.println("这是汽车的发动机");
        }
    }
    class Underpan {
        public void getStyle(){
            System.out.println("这是汽车的底盘");
        }
    }
    class Wheel {
        public void getStyle(){
            System.out.println("这是汽车的轮胎");
        }
    }
    interface ICar {
    	public void show();
    }
    
    class Car implements ICar {
    	private Engine engine;
        private Underpan underpan;
        private Wheel wheel;
    	
        @Override
        public void show() {
        	engine.getStyle();
        }
    	
    	
		public Car() {
			super();
		}


		public Car(Engine engine, Underpan underpan, Wheel wheel) {
			super();
			this.engine = engine;
			this.underpan = underpan;
			this.wheel = wheel;
		}
    	
    	
    }
    
    public class Client {
        public static void main(String[] args) {
            Engine engine = new Engine();
            Underpan underpan = new Underpan();
            Wheel wheel = new Wheel();
            ICar car = new Car(engine, underpan, wheel);
            car.show();
        }
    }