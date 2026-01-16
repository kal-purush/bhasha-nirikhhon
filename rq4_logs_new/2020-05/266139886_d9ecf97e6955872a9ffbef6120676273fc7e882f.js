function bounceOff(R1 ,R2) {

    if (R1.x - R2.x < R2.width/2 + R1.width/2
      && R2.x - R1.x < R2.width/2 + R1.width/2) {
    R1.velocityX = R1.velocityX * (-1);
    R2.velocityX = R2.velocityX * (-1);
  }
  if (R1.y - R2.y < R2.height/2 + R1.height/2
    && R2.y - R1.y < R2.height/2 + R1.height/2){
    R1.velocityY = R1.velocityY * (-1);
    R2.velocityY = R2.velocityY * (-1);
  }
  
  }
  