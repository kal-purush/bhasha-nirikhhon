package com.anaoliveiravictoriamartins.animaladoption;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Color;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.Toolbar;

import com.anaoliveiravictoriamartins.animaladoption.Domain.Entity.Animal;
import com.anaoliveiravictoriamartins.animaladoption.Repository.AnimalsRepository;

import java.util.List;

public class AnimalDetails extends AppCompatActivity {

    private ImageView imageAnimal;
    private TextView nameAnimal;
    private TextView raceAnimal;
    private TextView ageAnimal;
    private TextView weightAnimal;
    private TextView personalityAnimal;
    private Button button;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.animal_details);

        initialCustomization();

        int animalId = getIntent().getIntExtra("animal_id", -1);

        imageAnimal = findViewById(R.id.animalImg_details);
        nameAnimal = findViewById(R.id.animalName_details);
        raceAnimal = findViewById(R.id.animalRace_details);
        weightAnimal = findViewById(R.id.animalWeight_details);
        ageAnimal = findViewById(R.id.animalAge_details);
        personalityAnimal = findViewById(R.id.animalPersonality_details);
        button = findViewById(R.id.button);

        AnimalsRepository repository = AnimalsRepository.getInstance(this);
        Animal animal = repository.getById(animalId);

        Bitmap img = BitmapFactory.decodeFile(animal.UrlImage);

        imageAnimal.setImageBitmap(img);
        nameAnimal.setText(animal.Name);
        raceAnimal.setText(animal.Race);
        personalityAnimal.setText(animal.Personality);
        ageAnimal.setText(animal.Age > 1 ? animal.Age + " anos" : animal.Age + " ano");
        weightAnimal.setText(String.valueOf(animal.Weight) + " Kg");

        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                button.setEnabled(false);
                button.setAlpha(0.5f);

                animal.IsAdopted = true;
                repository.update(animal);

                Toast.makeText(v.getContext(), "O processo de adoção foi iniciado \uD83D\uDC3E", Toast.LENGTH_SHORT).show();
            }
        });

    }

    private void initialCustomization(){
        getWindow().setStatusBarColor(Color.parseColor("#17706e"));

        Toolbar toolbar = findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        if (getSupportActionBar() != null) {
            getSupportActionBar().setDisplayHomeAsUpEnabled(true);
            getSupportActionBar().setDisplayShowTitleEnabled(false);
        }
    }
}