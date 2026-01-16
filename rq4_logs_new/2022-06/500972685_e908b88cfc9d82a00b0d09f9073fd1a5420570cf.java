package com.nishtahir.androidrust;

public class LeafJni {

    // public static native int Java_com_nishtahir_androidrust_LeafJni_leaf_1run_1with_1options(
    private static native int leaf_run_with_options(
            short rt_id,
            String config_path,
            boolean auto_reload,
            boolean multi_thread,
            boolean auto_threads,
            int threads,
            int stack_size
    );

    static {
        try {
            System.loadLibrary("leafffi");
            int status = leaf_run_with_options(
                    (short) 1,
                    "config.toml",
                    false,
                    false,
                    false,
                    1,
                    100000
            );
            System.out.println("Leaf started successfully" +  status);
        } catch (UnsatisfiedLinkError unsatisfiedLinkError) {
            System.out.println(
                    "UnsatisfiedLinkError: " + unsatisfiedLinkError.getMessage()
            );
        }
    }
}