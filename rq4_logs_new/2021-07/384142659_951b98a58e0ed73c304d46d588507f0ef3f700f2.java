package com.example.gauntlet;

// This allows an alien to communicate with the game engine
// and spawn a arrow
interface AlienArrowSpawner {
    void spawnAlienArrow(Transform transform);
}