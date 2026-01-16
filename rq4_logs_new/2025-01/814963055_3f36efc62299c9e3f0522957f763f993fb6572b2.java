package com.example.multithreadingProject.FileSystem;

import com.example.multithreadingProject.FileSystem.model.Directory;
import com.example.multithreadingProject.FileSystem.model.File;

public class FileSystemApplication {
    public static void main(String[] args) {
        Directory movies = new Directory("movies");
        movies.addFileSystem(new File("Memento"));
        Directory comedyMovies = new Directory("comedyMovies");
        comedyMovies.addFileSystem(new File("Hulchul"));
        movies.addFileSystem(comedyMovies);

        movies.ls();
    }
}