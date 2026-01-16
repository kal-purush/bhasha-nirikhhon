package com.example.projek;

import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.PorterDuff;
import android.os.Bundle;
import android.view.SurfaceHolder;
import android.view.SurfaceView;
import android.view.View;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.AppCompatImageButton;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class GameActivity extends AppCompatActivity implements SurfaceHolder.Callback {
    private final List<SnakePoints> snakePointsList = new ArrayList<>();

    private SurfaceView surfaceView;
    private TextView scoreTv;
    private SurfaceHolder surfaceHolder;
    private String movingPosition = "right";
    private int score = 0;
    private static final int pointSize = 28;
    private static final int defaultTalePoints = 3;
    private static final int snakeColor = Color.YELLOW;
    private static final int snakeMovingSpeed = 800;
    private int positionX, positionY;
    private Timer timer;
    private Canvas canvas = null;
    private Paint pointColor = null;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_game);
        surfaceView = findViewById(R.id.surfaceView);
        scoreTv = findViewById(R.id.scoreTV);

        final AppCompatImageButton topBtn = findViewById(R.id.topBtn);
        final AppCompatImageButton leftBtn = findViewById(R.id.leftBtn);
        final AppCompatImageButton rightBtn = findViewById(R.id.rightBtn);
        final AppCompatImageButton bottomBtn = findViewById(R.id.bottomBtn);

        surfaceView.getHolder().addCallback(this);
        topBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (!movingPosition.equals("bottom")) {
                    movingPosition = "top";
                }
            }
        });

        leftBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (!movingPosition.equals("right")) {
                    movingPosition = "left";
                }
            }
        });

        rightBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (!movingPosition.equals("left")) {
                    movingPosition = "right";
                }
            }
        });

        bottomBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (!movingPosition.equals("top")) {
                    movingPosition = "bottom";
                }
            }
        });
    }

    @Override
    public void surfaceCreated(@NonNull SurfaceHolder surfaceHolder) {
        this.surfaceHolder = surfaceHolder;
        init();
    }

    @Override
    public void surfaceChanged(@NonNull SurfaceHolder holder, int format, int width, int height) {
    }

    @Override
    public void surfaceDestroyed(@NonNull SurfaceHolder holder) {
    }

    private void init() {
        snakePointsList.clear();
        scoreTv.setText("0");
        score = 0;
        movingPosition = "right";
        int startPositionX = (pointSize) * defaultTalePoints;

        for (int i = 0; i < defaultTalePoints; i++) {
            SnakePoints snakePoints = new SnakePoints(startPositionX, pointSize);
            snakePointsList.add(snakePoints);
            startPositionX -= (pointSize * 2);
        }

        addPoint();
        moveSnake();
    }

    private void addPoint() {
        int surfaceWidth = surfaceView.getWidth() - (pointSize * 2);
        int surfaceHeight = surfaceView.getHeight() - (pointSize * 2);

        int maxX = surfaceWidth / (pointSize * 2);
        int maxY = surfaceHeight / (pointSize * 2);

        Random random = new Random();
        int randomXPosition = random.nextInt(maxX);
        int randomYPosition = random.nextInt(maxY);

        positionX = randomXPosition * (pointSize * 2) + pointSize;
        positionY = randomYPosition * (pointSize * 2) + pointSize;
    }

    private void moveSnake() {
        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                int headPositionX = snakePointsList.get(0).getPositionX();
                int headPositionY = snakePointsList.get(0).getPositionY();

                if (headPositionX == positionX && headPositionY == positionY) {
                    growSnake();
                    addPoint();
                }

                switch (movingPosition) {
                    case "right":
                        snakePointsList.get(0).setPositionX(headPositionX + (pointSize * 2));
                        snakePointsList.get(0).setPositionY(headPositionY);
                        break;
                    case "left":
                        snakePointsList.get(0).setPositionX(headPositionX - (pointSize * 2));
                        snakePointsList.get(0).setPositionY(headPositionY);
                        break;
                    case "top":
                        snakePointsList.get(0).setPositionY(headPositionY - (pointSize * 2));
                        snakePointsList.get(0).setPositionX(headPositionX);
                        break;
                    case "bottom":
                        snakePointsList.get(0).setPositionY(headPositionY + (pointSize * 2));
                        snakePointsList.get(0).setPositionX(headPositionX);
                        break;
                }

                if (checkGameOver(snakePointsList.get(0).getPositionX(), snakePointsList.get(0).getPositionY())) {
                    timer.purge();
                    timer.cancel();

                    AlertDialog.Builder builder = new AlertDialog.Builder(GameActivity.this);
                    builder.setTitle("Game Over");
                    builder.setCancelable(false);

// Play Again Button
                    builder.setPositiveButton("Play Again", new DialogInterface.OnClickListener() {
                        @Override
                        public void onClick(DialogInterface dialog, int which) {
                            init();  // Restart the game
                        }
                    });

// View Score Button
                    builder.setNegativeButton("View Score", new DialogInterface.OnClickListener() {
                        @Override
                        public void onClick(DialogInterface dialog, int which) {
                            Toast.makeText(GameActivity.this, "Give up already ?", Toast.LENGTH_SHORT).show();
                            // Navigate to ScoreActivity
                            Intent intent = new Intent(GameActivity.this, ScoreActivity.class);
                            intent.putExtra("score", score);
                            startActivity(intent);
                            finish();  // Close current game activity
                        }
                    });
                    runOnUiThread(new Runnable() {
                        @Override
                        public void run() {
                            builder.show();
                        }
                    });
                } else {
                    canvas = surfaceHolder.lockCanvas();
                    canvas.drawColor(Color.WHITE, PorterDuff.Mode.CLEAR);
                    canvas.drawCircle(snakePointsList.get(0).getPositionX(), snakePointsList.get(0).getPositionY(), pointSize, createPointColor());
                    canvas.drawCircle(positionX, positionY, pointSize, createPointColor());

                    for (int i = 1; i < snakePointsList.size(); i++) {
                        int getTempPositionX = snakePointsList.get(i).getPositionX(); // Corrected line
                        int getTempPositionY = snakePointsList.get(i).getPositionY();

                        snakePointsList.get(i).setPositionX(headPositionX);
                        snakePointsList.get(i).setPositionY(headPositionY);
                        canvas.drawCircle(snakePointsList.get(i).getPositionX(), snakePointsList.get(i).getPositionY(), pointSize, createPointColor());

                        headPositionX = getTempPositionX;
                        headPositionY = getTempPositionY;
                    }

                    surfaceHolder.unlockCanvasAndPost(canvas);
                }
            }
        }, 1000 - snakeMovingSpeed, 1000 - snakeMovingSpeed);
    }

    private void growSnake() {
        if (!snakePointsList.isEmpty()) {
            SnakePoints lastPoint = snakePointsList.get(snakePointsList.size() - 1);
            snakePointsList.add(new SnakePoints(lastPoint.getPositionX(), lastPoint.getPositionY()));
        } else {
            snakePointsList.add(new SnakePoints(0, 0));
        }
        score++;
        runOnUiThread(() -> scoreTv.setText(String.valueOf(score)));
    }

    private boolean checkGameOver(int headPositionX, int headPositionY) {
        boolean gameOver = false;
        if (headPositionX < 0 ||
                headPositionY < 0 ||
                headPositionX >= surfaceView.getWidth() ||
                headPositionY >= surfaceView.getHeight()) {
            gameOver = true;
        } else {
            for (int i = 1; i < snakePointsList.size(); i++) {
                if (headPositionX == snakePointsList.get(i).getPositionX() &&
                        headPositionY == snakePointsList.get(i).getPositionY()) {
                    gameOver = true;
                    break;
                }
            }
        }
        return gameOver;
    }

    private Paint createPointColor() {
        if (pointColor == null) {
            pointColor = new Paint();
            pointColor.setColor(snakeColor);
            pointColor.setStyle(Paint.Style.FILL);
            pointColor.setAntiAlias(true);
        }
        return pointColor;
    }
}