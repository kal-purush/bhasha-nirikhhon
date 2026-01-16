package org.telran.lecture_14_hash.AVLTree;

class Node {
    int key;
    String value;
    Node left;
    Node right;
    Node parent;
    int balanceFactor;

    /**
     * Конструктор для создания нового узла.
     *
     * @param key   Ключ узла.
     * @param value Значение узла.
     */
    public Node(int key, String value) {
        this.key = key;
        this.value = value;
        this.left = null;
        this.right = null;
        this.parent = null;
        this.balanceFactor = 0;
    }
}

public class AVLTree {

    Node root;
    private int length = 0;
    private boolean isBalanced;
    boolean showDetails;

    /**
     * Конструктор для создания нового бинарного дерева поиска.
     */
    public AVLTree() {
        this.root = null;
        this.isBalanced = true;
        this.showDetails = false;
    }

    public AVLTree(boolean showDetails) {
        this.root = null;
        this.isBalanced = true;
        this.showDetails = showDetails;
    }

    /**
     * Вставляет новый узел в дерево.
     *
     * @param key   Ключ нового узла.
     * @param value Значение нового узла.
     */
    public void insert(int key, String value) {
        Node newNode = new Node(key, value);
        if (root == null) {
            root = newNode;
            length++;
        } else {
            insertNode(root, newNode);
        }
    }


    /**
     * Рекурсивно вставляет новый узел в поддерево.
     *
     * @param node    Текущий узел.
     * @param newNode Новый узел.
     */
    private void insertNode(Node node, Node newNode) {
        if (newNode.key < node.key) {
            if (node.left == null) {
                node.left = newNode;
                newNode.parent = node;
                length++;
                updateBalancePath(newNode); // Балансируем от newNode вверх
            } else {
                insertNode(node.left, newNode);
            }
        } else if (newNode.key > node.key) {
            if (node.right == null) {
                node.right = newNode;
                newNode.parent = node;
                length++;
                updateBalancePath(newNode); // Балансируем от newNode вверх
            } else {
                insertNode(node.right, newNode);
            }
        } else { // Найдена дублирующая Нода
            node.value = newNode.value;
        }
    }

    /**
     * Ищет значение узла по заданному ключу.
     *
     * @param key Ключ искомого узла.
     * @return Значение найденного узла или null, если узел не найден.
     */
    public String search(int key) {
        Node node = searchNode(root, key);
        return node != null ? node.value : null;
    }

    /**
     * Рекурсивно ищет узел с заданным ключом в поддереве.
     *
     * @param node Текущий узел.
     * @param key  Ключ искомого узла.
     * @return Найденный узел или null, если узел не найден.
     */
    private Node searchNode(Node node, int key) {
        if (node == null) {
            return null;
        }

        if (key < node.key) {
            return this.searchNode(node.left, key);
        } else if (key > node.key) {
            return this.searchNode(node.right, key);
        } else {
            return node;
        }
    }

    /**
     * Находит минимальный узел в дереве.
     *
     * @return Минимальный узел или null, если дерево пустое.
     */
    public Node min() {
        if (root == null) {
            return null;
        }
        Node currentNode = root;
        while (currentNode.left != null) {
            currentNode = currentNode.left;
        }
        return currentNode;
    }

    private Node min(Node node) {
        while (node.left != null) {
            node = node.left;
        }
        return node;
    }

    /**
     * Находит максимальный узел в дереве.
     *
     * @return Максимальный узел или null, если дерево пустое.
     */
    public Node max() {
        // TODO-5: напишите реализацию метода
        return new Node(0, "zero"); // Заглушка
    }

    public int length() {
        return length;
    }


    private void updateBalancePath(Node node) {
        while (node != null) {
            updateBalanceFactor(node);
            balance(node);
            node = node.parent;
        }
    }

    private void updateBalanceFactor(Node node) {
        node.balanceFactor = getNodeHeight(node.right) - getNodeHeight(node.left);
    }

    private int getNodeHeight(Node node) {
        if (node == null) {
            return 0;
        }
        return Math.max(getNodeHeight(node.left), getNodeHeight(node.right)) + 1;
    }

    public boolean isBalanced() {
        return this.isBalanced;
    }

    private void balance(Node node) {
        if (node == null) {
            return;
        }

        if (node.balanceFactor > 1) {
            if (node.right.balanceFactor < 0) {
                node.right = rightRotate(node.right);
            }
            node = leftRotate(node);
        } else if (node.balanceFactor < -1) {
            if (node.left.balanceFactor > 0) {
                node.left = leftRotate(node.left);
            }
            node = rightRotate(node);
        }

        // Обновляем баланс фактор родителя после поворота.
        if (node.left != null) {
            updateBalanceFactor(node.left);
        }
        if (node.right != null) {
            updateBalanceFactor(node.right);
        }
        updateBalanceFactor(node);

        // Если node был корнем, то необходимо обновить root.
        if (node.parent == null) {
            root = node;
        }
    }

    private Node leftRotate(Node node) {
        if (showDetails) {
            System.out.println("leftRotate");
            displayTree();
        }

        Node rightChild = node.right;
        node.right = rightChild.left;

        if (rightChild.left != null) {
            rightChild.left.parent = node;
        }

        rightChild.left = node;
        rightChild.parent = node.parent;
        node.parent = rightChild;

        if (rightChild.parent != null) {
            if (rightChild.parent.left == node) {
                rightChild.parent.left = rightChild;
            } else {
                rightChild.parent.right = rightChild;
            }
        }

        updateBalanceFactor(node);
        updateBalanceFactor(rightChild);

        return rightChild;
    }

    private Node rightRotate(Node node) {
        if (showDetails) {
            System.out.println("rightRotate");
            displayTree();
        }

        Node leftChild = node.left;
        node.left = leftChild.right;

        if (leftChild.right != null) {
            leftChild.right.parent = node;
        }

        leftChild.right = node;
        leftChild.parent = node.parent;
        node.parent = leftChild;

        if (leftChild.parent != null) {
            if (leftChild.parent.left == node) {
                leftChild.parent.left = leftChild;
            } else {
                leftChild.parent.right = leftChild;
            }
        }

        updateBalanceFactor(node);
        updateBalanceFactor(leftChild);

        return leftChild;
    }

    public void remove(int key) {
        Node nodeToRemove = searchNode(root, key);
        if (nodeToRemove == null) {
            return; // Узел не найден
        }
        removeNode(nodeToRemove);
    }

    private void removeNode(Node nodeToRemove) {
        Node replacementNode;
        Node parent = nodeToRemove.parent;

        if (nodeToRemove.left == null || nodeToRemove.right == null) {
            replacementNode = nodeToRemove.left != null ? nodeToRemove.left : nodeToRemove.right;
        } else {
            replacementNode = min(nodeToRemove.right);
            nodeToRemove.key = replacementNode.key;
            nodeToRemove.value = replacementNode.value;
            removeNode(replacementNode); // Рекурсивно удаляем узел замены
            updateBalancePath(nodeToRemove);
            length--;
            return;
        }

        if (replacementNode != null) {
            replacementNode.parent = parent;
        }

        if (parent == null) {
            root = replacementNode;
        } else if (parent.left == nodeToRemove) {
            parent.left = replacementNode;
        } else {
            parent.right = replacementNode;
        }

        updateBalancePath(parent);
        length--;
    }


    /**
     * Отображает дерево в виде древовидной структуры в терминале.
     */
    public void displayTree() {
        if (root == null) {
            System.out.println("Дерево пустое.");
            return;
        }
        displayNode(root, "", true);
    }

    /**
     * Рекурсивно отображает узел и его поддеревья.
     *
     * @param node   Текущий узел.
     * @param prefix Префикс для отображения уровня дерева.
     * @param isLeft Флаг, указывающий, является ли узел левым дочерним.
     */
    private void displayNode(Node node, String prefix, boolean isLeft) {
        if (node != null) {
            String displayPrefix = prefix + (isLeft ? "├── " : "└── ");
            String directionIndicator = isLeft ? "L: " : "R: ";

            System.out.println(displayPrefix + directionIndicator + node.key + ":" + node.balanceFactor);

            String childPrefix = prefix + (isLeft ? "│   " : "    ");
            if (node.left != null || node.right != null) {
                displayNode(node.left, childPrefix, true);
                displayNode(node.right, childPrefix, false);
            }
        }
    }

    public static void main(String[] args) {
        AVLTree tree = new AVLTree();
        tree.insert(50, "1");
        tree.insert(60, "1");
        tree.insert(10, "1");
        tree.insert(8, "1");
        tree.insert(20, "1");
        tree.insert(15, "1");

        System.out.println("result tree");
        tree.displayTree();

        tree.remove(50);
        tree.displayTree();

    }

}