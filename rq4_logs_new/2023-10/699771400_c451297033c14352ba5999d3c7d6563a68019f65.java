package org.example.model.repository.stock;


import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.MongoDatabase;
import org.example.model.domain.entity.Decoration;
import org.example.model.domain.entity.Flower;
import org.example.model.domain.entity.Product;
import org.example.model.domain.entity.Tree;
import org.example.model.repository.DatabaseConnection;
import org.example.model.repository.interfaces.StockDAO;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

public class StockMongoDAO implements StockDAO {

    private static final String COLLECTION_NAME = "stockList";
    private MongoCollection<Document> productCollection;

    public StockMongoDAO() {
        MongoDatabase database = DatabaseConnection.getConnection();//Connection pooling is a standard leaving connection open in mongo
        productCollection = database.getCollection(COLLECTION_NAME);
    }

    @Override
    public List<Product> findAll() {
        List<Product> stockList = new ArrayList<>();
        MongoCursor<Document> cursor = productCollection.find().iterator();
        try {
            while (cursor.hasNext()) {
                Document productDoc = cursor.next();
                stockList.add(mapDocumentToProduct(productDoc));
            }
        } finally {
            cursor.close();
        }
        return stockList;
    }

    @Override
    public Product findById(int id) {
        Document query = new Document("id", id);
        Document productDoc = productCollection.find(query).first();
        if (productDoc != null) {
            return mapDocumentToProduct(productDoc);
        }
        return null;
    }

    @Override
    public List<Product> findByName(String name) {
        Document query = new Document("name", name);
        List<Product> stockList = new ArrayList<>();
        productCollection.find(query).forEach(document -> stockList.add(mapDocumentToProduct(document)));
        return stockList;
    }

    @Override
    public List<Product> findByPrice(double price) {
        Document query = new Document("price", price);
        List<Product> stockList = new ArrayList<>();
        productCollection.find(query).forEach(document -> stockList.add(mapDocumentToProduct(document)));
        return stockList;
    }

    @Override
    public List<Product> findByType(String productType) {
        Document query = new Document("type", productType);
        List<Product> stockList = new ArrayList<>();
        productCollection.find(query).forEach(document -> stockList.add(mapDocumentToProduct(document)));
        return stockList;
    }

    @Override
    public List<Product> findByHeight(float height) {
        Document query = new Document("height", height);
        List<Product> stockList = new ArrayList<>();
        productCollection.find(query).forEach(document -> stockList.add(mapDocumentToProduct(document)));
        return stockList;
    }

    @Override
    public List<Product> findByColor(String color) {
        Document query = new Document("color", color);
        List<Product> stockList = new ArrayList<>();
        productCollection.find(query).forEach(document -> stockList.add(mapDocumentToProduct(document)));
        return stockList;
    }

    @Override
    public List<Product> findByMaterial(boolean material) {
        Document query = new Document("material_is_wood", material);
        List<Product> stockList = new ArrayList<>();
        productCollection.find(query).forEach(document -> stockList.add(mapDocumentToProduct(document)));
        return stockList;
    }

    @Override
    public boolean insertProduct(Product p) {
        Document productDoc = mapProductToDocument(p);
        productCollection.insertOne(productDoc);
        return true;
    }

    @Override
    public boolean updateProduct(Product p) {
        Document query = new Document("id", p.getId());
        Document productDoc = mapProductToDocument(p);
        productCollection.updateOne(query, new Document("$set", productDoc));//$set operator to set the fields in the document to the values provided in productDoc
        return true;
    }

    @Override
    public boolean deleteProduct(int id) {
        Document query = new Document("id", id);
        productCollection.deleteOne(query);
        return true;
    }

    private Product mapDocumentToProduct(Document productDoc) {
        int id = productDoc.getInteger("id");
        String name = productDoc.getString("name");
        double price = productDoc.getDouble("price");
        String type = productDoc.getString("type");
        int invoiceId = productDoc.getInteger("invoiceId");

        switch (type) {
            case "Tree":
                float height = productDoc.getDouble("height").floatValue();
                return new Tree(id, name, price, height, invoiceId);

            case "Flower":
                String color = productDoc.getString("color");
                return new Flower(id, name, price, color, invoiceId);

            case "Decoration":
                boolean materialIsWood = productDoc.getBoolean("materialIsWood");
                return new Decoration(id, name, price, materialIsWood, invoiceId);
        }
        return null;
    }

    private Document mapProductToDocument(Product p) {
        Document productDoc = new Document("id", p.getId())
                .append("name", p.getName())
                .append("price", p.getPrice())
                .append("type", p.getType())
                .append("invoiceId", p.getInvoiceId());

        if (p instanceof Tree) {
            Tree tree = (Tree) p;
            productDoc.append("height", tree.getHeight());
        } else if (p instanceof Flower) {
            Flower flower = (Flower) p;
            productDoc.append("color", flower.getColor());
        } else if (p instanceof Decoration) {
            Decoration decoration = (Decoration) p;
            productDoc.append("materialIsWood", decoration.isMaterialIsWood());
        }

        return productDoc;
    }

}