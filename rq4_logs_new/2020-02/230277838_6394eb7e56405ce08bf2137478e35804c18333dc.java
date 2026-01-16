package ru.epam.javacore.lesson_22_annotations.lesson.a_2_transaction;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Optional;

public class A_2_TransactionManager {

    @FunctionalInterface
    private interface Transactive {
        boolean doInTransaction();
    }

    public void transaction_1(Transactive body) {
        System.out.println("Transaction begin. Open db connection");
        boolean result = body.doInTransaction();
        System.out.println("Transaction end. Close db connection");
    }

    public void transaction_2(Transactive body) {
        System.out.println("Transaction begin. Open db connection");
        int g = 44;
        boolean result = body.doInTransaction();
        System.out.println("Transaction end. Close db connection");
    }

    static int i = 1;

    @Transactional( errorMessage = "OOPS something went wrong!")
    public void transaction_3() {
        body(i);
    }

    private void body(int i) {
        System.out.println("Body in action");
        if (i == 1) {
            A_2_TransactionManager.i++;
            throw new RuntimeException("Error while transaction in progress");
        }
    }

    public void transaction_3_Proxy() throws Exception {

        Method method = this.getClass()
                            .getMethod("transaction_3");
        Optional<Transactional> annotationFromMethod = getAnnotationFromMethod(method);

        if (annotationFromMethod.isPresent()) {
            Transactional transactional = annotationFromMethod.get();
            int numberOfRepeat = transactional.repeatIfError();

            System.out.println("Transaction begin. Open db connection");
            boolean success = false;
            if (numberOfRepeat == 0) {
                method.invoke(this);
            } else {
                for (int i = 0; i < transactional.repeatIfError(); i++) {
                    try {
                        method.invoke(this);
                        success = true;
                        if (success) {
                            break;
                        }
                    } catch (Exception e) {
                        System.out.println("Error " + transactional.errorMessage());
                    }
                }
            }
            System.out.println("Transaction end. Close db connection");
        }
    }

    private Optional<Transactional> getAnnotationFromMethod(Method method) {
        Annotation[] annotations = method.getAnnotations();

        Optional<Transactional> transactionalAnnotationOptional = Optional.empty();

        if (method.isAnnotationPresent((Transactional.class))) {
            transactionalAnnotationOptional =
                    Optional.of(method.getAnnotation(Transactional.class));
        }


        return transactionalAnnotationOptional;
    }

    public static void main(String[] args) throws Exception {
        A_2_TransactionManager transactionManager = new A_2_TransactionManager();
        /*transactionManager.transaction_1(() -> {
            System.out.println("Transaction body");
            return true;
        });*/

        transactionManager.transaction_3_Proxy();
    }
}