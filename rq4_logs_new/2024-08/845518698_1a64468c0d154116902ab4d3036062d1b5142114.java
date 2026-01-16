package myApp.service;

public class SqlQueries {
    public static final String SAVE_CLIENT = "INSERT INTO public.clients (id, name, creation_date, status) VALUES (?, ?, ?, ?)";
    public static final String GET_CLIENT_BY_ID = "SELECT * FROM public.clients WHERE id = ?";
    public static final String UPDATE_CLIENT_STATUS = "UPDATE public.clients SET status = ? WHERE id = ?";
    public static final String SAVE_ORDER = "INSERT INTO public.orders (id, client_id, creation_date, total_sum) VALUES (?, ?, ?, ?)";
    public static final String GET_ORDER_BY_ID = "SELECT * FROM public.orders WHERE id = ?";
}