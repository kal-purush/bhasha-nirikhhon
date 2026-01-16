    package org.flowershop.repository;

    import org.flowershop.domain.products.Product;
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.PreparedStatement;
    import java.sql.SQLException;
    import java.util.List;

    public class ProductRepositorySQL implements IProductRepository {

        private Connection connection;

        public ProductRepositorySQL() {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/Flower_Shop", "root", "");
            } catch (ClassNotFoundException ex) {
                System.out.println("Error al registrar el driver de MySQL: " + ex);
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }


        @Override
        public void addProduct(Product product) {
            String query = "INSERT INTO products (name, price, stock) VALUES (?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setString(1, product.getName());
                statement.setDouble(2, product.getPrice());
                statement.setInt(3, product.getStock());
                statement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void updateProductById(long id, Product product) {
            String query = "UPDATE products SET name = ?, price = ?, stock = ? WHERE id = ?";
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setString(1, product.getName());
                statement.setDouble(2, product.getPrice());
                statement.setInt(3, product.getStock());
                statement.setLong(4, id);
                statement.executeUpdate();
            }catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void removeProductById(long id){
            String query = "DELETE FROM products WHERE id = ?";
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setLong(1, id);
                statement.executeUpdate();
            }catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        @Override
        public Product getProductById(long id){
            return null;
        }

        @Override
        public void saveProducts() {
            // Implementar el guardado de productos si es necesario
        }

        @Override
        public void loadProducts() {
            // Implementar la carga de productos si es necesario
        }

        @Override
        public Product getProductByRef(String ref) {
            // Implementar la obtención de productos por referencia si es necesario
            return null;
        }

        @Override
        public List<Product> getProducts(){
            return null;
        }

        @Override
        public void removeProductByRef(String ref) {
            // Implementar la eliminación de productos por referencia si es necesario
        }
    }