using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using Xunit;
using EasySave.Model;

namespace EasySave.Tests.Model {
    public class DirectoryHandlerTests {
        private readonly string _testDirectoryPath = "TestDirectory";

        public DirectoryHandlerTests() {
            // Ensure a clean state before each test
            if (Directory.Exists(_testDirectoryPath)) {
                Directory.Delete(_testDirectoryPath, true);
            }
        }

        [Fact]
        public void GetName_ShouldReturnDirectoryName() {
            // Arrange
            Directory.CreateDirectory(_testDirectoryPath);
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);

            // Act
            var result = directoryHandler.GetName();

            // Assert
            Assert.Equal("TestDirectory", result);
        }

        [Fact]
        public void GetSize_ShouldReturnDirectorySize_WhenDirectoryExists() {
            // Arrange
            Directory.CreateDirectory(_testDirectoryPath);
            File.WriteAllText(Path.Combine(_testDirectoryPath, "file1.txt"), "Test content");
            File.WriteAllText(Path.Combine(_testDirectoryPath, "file2.txt"), "More content");
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);

            // Act
            var result = directoryHandler.GetSize();

            // Assert
            var expectedSize = Directory.GetFiles(_testDirectoryPath).Sum(file => new FileInfo(file).Length);
            Assert.Equal(expectedSize, result);
        }

        [Fact]
        public void GetSize_ShouldThrowDirectoryNotFoundException_WhenDirectoryDoesNotExist() {
            // Arrange
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);

            // Act & Assert
            Assert.Throws<DirectoryNotFoundException>(() => directoryHandler.GetSize());
        }

        [Fact]
        public void Remove_ShouldDeleteDirectory_WhenDirectoryExists() {
            // Arrange
            Directory.CreateDirectory(_testDirectoryPath);
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);

            // Act
            directoryHandler.Remove();

            // Assert
            Assert.False(Directory.Exists(_testDirectoryPath));
        }

        [Fact]
        public void Remove_ShouldThrowDirectoryNotFoundException_WhenDirectoryDoesNotExist() {
            // Arrange
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);

            // Act & Assert
            Assert.Throws<DirectoryNotFoundException>(() => directoryHandler.Remove());
        }

        [Fact]
        public void Move_ShouldMoveDirectoryToNewLocation() {
            // Arrange
            var destinationPath = "MovedDirectory";
            Directory.CreateDirectory(_testDirectoryPath);
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);
            var destinationHandler = new DirectoryHandler(destinationPath);

            // Act
            directoryHandler.Move(destinationHandler);

            // Assert
            Assert.False(Directory.Exists(_testDirectoryPath));
            Assert.True(Directory.Exists(destinationPath));

            // Cleanup
            Directory.Delete(destinationPath, true);
        }

        [Fact]
        public void Copy_ShouldCopyDirectoryToNewLocation() {
            // Arrange
            var destinationPath = "CopiedDirectory";
            Directory.CreateDirectory(_testDirectoryPath);
            File.WriteAllText(Path.Combine(_testDirectoryPath, "file1.txt"), "Test content");
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);
            var destinationHandler = new DirectoryHandler(destinationPath);

            // Act
            directoryHandler.Copy(destinationHandler);

            // Assert
            Assert.True(Directory.Exists(_testDirectoryPath));
            Assert.True(Directory.Exists(destinationPath));
            Assert.True(File.Exists(Path.Combine(destinationPath, "file1.txt")));

            // Cleanup
            Directory.Delete(destinationPath, true);
        }

        [Fact]
        public void Rename_ShouldRenameDirectory() {
            // Arrange
            var newName = "RenamedDirectory";
            Directory.CreateDirectory(_testDirectoryPath);
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);

            // Act
            directoryHandler.Rename(newName);

            // Assert
            Assert.False(Directory.Exists(_testDirectoryPath));
            Assert.True(Directory.Exists(newName));

            // Cleanup
            Directory.Delete(newName, true);
        }

        [Fact]
        public void Exists_ShouldReturnTrue_WhenDirectoryExists() {
            // Arrange
            Directory.CreateDirectory(_testDirectoryPath);
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);

            // Act
            var result = directoryHandler.Exists();

            // Assert
            Assert.True(result);
        }

        [Fact]
        public void Exists_ShouldReturnFalse_WhenDirectoryDoesNotExist() {
            // Arrange
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);

            // Act
            var result = directoryHandler.Exists();

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void GetEntries_ShouldReturnListOfEntries_WhenDirectoryExists() {
            // Arrange
            Directory.CreateDirectory(_testDirectoryPath);
            File.WriteAllText(Path.Combine(_testDirectoryPath, "file1.txt"), "Test content");
            Directory.CreateDirectory(Path.Combine(_testDirectoryPath, "SubDirectory"));
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);

            // Act
            var entries = directoryHandler.GetEntries();

            // Assert
            Assert.Equal(2, entries.Count);
            Assert.Contains(entries, entry => entry.GetName() == "file1.txt");
            Assert.Contains(entries, entry => entry.GetName() == "SubDirectory");
        }

        [Fact]
        public void GetEntries_ShouldThrowDirectoryNotFoundException_WhenDirectoryDoesNotExist() {
            // Arrange
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);

            // Act & Assert
            Assert.Throws<DirectoryNotFoundException>(() => directoryHandler.GetEntries());
        }

        [Fact]
        public void GetParent_ShouldReturnParentDirectory() {
            // Arrange
            Directory.CreateDirectory(_testDirectoryPath);
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);

            // Act
            var parent = directoryHandler.GetParent();

            // Assert
            Assert.Equal(Directory.GetParent(_testDirectoryPath)?.FullName, parent.GetPath());
        }

        [Fact]
        public void GetParent_ShouldThrowDirectoryNotFoundException_WhenDirectoryDoesNotExist() {
            // Arrange
            var directoryHandler = new DirectoryHandler(_testDirectoryPath);

            // Act & Assert
            Assert.Throws<DirectoryNotFoundException>(() => directoryHandler.GetParent());
        }
    }
}