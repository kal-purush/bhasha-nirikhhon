package ru.moore;

public class Class1 {

    @BeforeSuite
    public void beforeSuite1(){
        System.out.println(this.getClass().getSimpleName()+" method beforeSuite1");
    }

    public void beforeSuite2(){
        System.out.println(this.getClass().getSimpleName()+" method beforeSuite2");
    }

    @Test(priority = 5)
    public void test1(){
        System.out.println(this.getClass().getSimpleName()+" method test1, priority 5");
    }

    @Test(priority = 1)
    public void test2(){
        System.out.println(this.getClass().getSimpleName()+" method test2, priority 1");
    }

    @Test(priority = 9)
    public void test3(){
        System.out.println(this.getClass().getSimpleName()+" method test3, priority 9");
    }

    @Test
    public void test4() {
        System.out.println(this.getClass().getSimpleName()+" method test4, priority default");
    }

    @AfterSuite
    public void afterSuite(){
        System.out.println(this.getClass().getSimpleName()+" method afterSuite");
    }
}