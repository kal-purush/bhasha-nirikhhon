package com.example;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Clase de prueba para la clase BinaryTree.
 */
public class BinaryTreeTest {

    /**
     * Prueba para el método insert de la clase BinaryTree.
     */
    @Test
    void testInsert() {
        // Crear un árbol binario de tipo Integer
        BinaryTree<Integer> tree = new BinaryTree<>();
        
        // Insertar elementos en el árbol
        tree.insert(5);
        tree.insert(3);
        tree.insert(7);

        // Verificar si los elementos se insertaron correctamente y si la búsqueda funciona correctamente
        assertTrue(tree.search(5));
        assertTrue(tree.search(3));
        assertTrue(tree.search(7));
        assertFalse(tree.search(10)); // 10 no debería estar en el árbol
    }

    /**
     * Prueba para el método search de la clase BinaryTree.
     */
    @Test
    void testSearch() {
        // Crear un árbol binario de tipo Integer
        BinaryTree<Integer> tree = new BinaryTree<>();
        
        // Insertar elementos en el árbol
        tree.insert(5);
        tree.insert(3);
        tree.insert(7);

        // Verificar si los elementos se insertaron correctamente y si la búsqueda funciona correctamente
        assertTrue(tree.search(5));
        assertTrue(tree.search(3));
        assertTrue(tree.search(7));
        assertFalse(tree.search(10)); // 10 no debería estar en el árbol
    }
}