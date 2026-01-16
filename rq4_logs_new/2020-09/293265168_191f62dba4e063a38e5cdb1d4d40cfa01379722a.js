var p5Inst = new p5(null, 'sketch');

window.preload = function () {
  initMobileControls(p5Inst);

  p5Inst._predefinedSpriteAnimations = {};
  p5Inst._pauseSpriteAnimationsByDefault = false;
  var animationListJSON = {"orderedKeys":["d645f530-e3ec-4ecc-8fcb-1f5734df6a0c","6132920a-848a-4f14-91bd-dd81a99bbcbd","5ce44e39-12ac-4a66-88cf-a87a0ed6a180","cb6da69c-12d1-4f53-9556-d93ff4807589","33841f90-7a53-4346-b956-e51d1961959b","f59671f4-65b0-445e-9792-70e8f3d79493","1cc3ef39-8da9-47fb-9770-6b1f69822b00","3fb3561f-fec0-4ee8-a15c-3bcee5c46424","a3ef3e7c-79ff-43de-b4a0-b4704f1c6345","8b265dd5-561f-41d9-a567-b500ae2d07d8"],"propsByKey":{"d645f530-e3ec-4ecc-8fcb-1f5734df6a0c":{"name":"monkey","sourceUrl":null,"frameSize":{"x":560,"y":614},"frameCount":11,"looping":true,"frameDelay":2,"version":"X1xu987h.kCoxOlNpPKSg9PXhKd4q9Xs","loadedFromSource":true,"saved":true,"sourceSize":{"x":1680,"y":2456},"rootRelativePath":"assets/d645f530-e3ec-4ecc-8fcb-1f5734df6a0c.png"},"6132920a-848a-4f14-91bd-dd81a99bbcbd":{"name":"dead","sourceUrl":null,"frameSize":{"x":560,"y":614},"frameCount":1,"looping":true,"frameDelay":12,"version":"hnMdBAOiX6QTjWN2lC_g2FvnBWXRTJ_g","loadedFromSource":true,"saved":true,"sourceSize":{"x":560,"y":614},"rootRelativePath":"assets/6132920a-848a-4f14-91bd-dd81a99bbcbd.png"},"5ce44e39-12ac-4a66-88cf-a87a0ed6a180":{"name":"banana1","sourceUrl":"assets/v3/animations/0Pmc2UypwJxUUUBBxMOOYmiSvh97BJLRo_BQZbjyEto/5ce44e39-12ac-4a66-88cf-a87a0ed6a180.png","frameSize":{"x":1080,"y":1080},"frameCount":1,"looping":true,"frameDelay":4,"version":"EbInYXgRSzmcm3GFe.oPjpAAayum1wdw","loadedFromSource":true,"saved":true,"sourceSize":{"x":1080,"y":1080},"rootRelativePath":"assets/v3/animations/0Pmc2UypwJxUUUBBxMOOYmiSvh97BJLRo_BQZbjyEto/5ce44e39-12ac-4a66-88cf-a87a0ed6a180.png"},"cb6da69c-12d1-4f53-9556-d93ff4807589":{"name":"banana2","sourceUrl":"assets/v3/animations/0Pmc2UypwJxUUUBBxMOOYmiSvh97BJLRo_BQZbjyEto/5ce44e39-12ac-4a66-88cf-a87a0ed6a180.png","frameSize":{"x":1080,"y":1080},"frameCount":1,"looping":true,"frameDelay":4,"version":null,"loadedFromSource":true,"saved":true,"sourceSize":{"x":1080,"y":1080},"rootRelativePath":"assets/v3/animations/0Pmc2UypwJxUUUBBxMOOYmiSvh97BJLRo_BQZbjyEto/5ce44e39-12ac-4a66-88cf-a87a0ed6a180.png"},"33841f90-7a53-4346-b956-e51d1961959b":{"name":"Stone1","sourceUrl":"assets/v3/animations/0Pmc2UypwJxUUUBBxMOOYmiSvh97BJLRo_BQZbjyEto/33841f90-7a53-4346-b956-e51d1961959b.png","frameSize":{"x":512,"y":512},"frameCount":1,"looping":true,"frameDelay":4,"version":"dM_Mc2KCV96QgSOwaSXKuLo_Aq2vbxRB","loadedFromSource":true,"saved":true,"sourceSize":{"x":512,"y":512},"rootRelativePath":"assets/v3/animations/0Pmc2UypwJxUUUBBxMOOYmiSvh97BJLRo_BQZbjyEto/33841f90-7a53-4346-b956-e51d1961959b.png"},"f59671f4-65b0-445e-9792-70e8f3d79493":{"name":"Stone2","sourceUrl":null,"frameSize":{"x":512,"y":512},"frameCount":1,"looping":true,"frameDelay":12,"version":"rCtTGJ0UdvrvPguJy_IXpjoV3vVbpI6p","loadedFromSource":true,"saved":true,"sourceSize":{"x":512,"y":512},"rootRelativePath":"assets/f59671f4-65b0-445e-9792-70e8f3d79493.png"},"1cc3ef39-8da9-47fb-9770-6b1f69822b00":{"name":"invisibleGround","sourceUrl":"assets/v3/animations/KYxkIr1gwHX9brJlTDZ79Itzr8mxYk_WmrQuNNICYiM/1cc3ef39-8da9-47fb-9770-6b1f69822b00.png","frameSize":{"x":96,"y":1},"frameCount":1,"looping":true,"frameDelay":4,"version":"8KzxGoOn_v9nvRzzkyTN8c6OEf7my.tV","loadedFromSource":true,"saved":true,"sourceSize":{"x":96,"y":1},"rootRelativePath":"assets/v3/animations/KYxkIr1gwHX9brJlTDZ79Itzr8mxYk_WmrQuNNICYiM/1cc3ef39-8da9-47fb-9770-6b1f69822b00.png"},"3fb3561f-fec0-4ee8-a15c-3bcee5c46424":{"name":"gameOver","sourceUrl":"assets/api/v1/animation-library/gamelab/jlwUdmDfQ.Fl8uZni7e_c3sVaNJCXBYL/category_gameplay/textGameOver.png","frameSize":{"x":412,"y":78},"frameCount":1,"looping":true,"frameDelay":2,"version":"jlwUdmDfQ.Fl8uZni7e_c3sVaNJCXBYL","loadedFromSource":true,"saved":true,"sourceSize":{"x":412,"y":78},"rootRelativePath":"assets/api/v1/animation-library/gamelab/jlwUdmDfQ.Fl8uZni7e_c3sVaNJCXBYL/category_gameplay/textGameOver.png"},"a3ef3e7c-79ff-43de-b4a0-b4704f1c6345":{"name":"ground","sourceUrl":"assets/v3/animations/KYxkIr1gwHX9brJlTDZ79Itzr8mxYk_WmrQuNNICYiM/a3ef3e7c-79ff-43de-b4a0-b4704f1c6345.png","frameSize":{"x":2377,"y":12},"frameCount":1,"looping":true,"frameDelay":4,"version":"ovn12zA0Jz2l1rqoex4VG.b5ryQXBZ._","loadedFromSource":true,"saved":true,"sourceSize":{"x":2377,"y":12},"rootRelativePath":"assets/v3/animations/KYxkIr1gwHX9brJlTDZ79Itzr8mxYk_WmrQuNNICYiM/a3ef3e7c-79ff-43de-b4a0-b4704f1c6345.png"},"8b265dd5-561f-41d9-a567-b500ae2d07d8":{"name":"animation_1","sourceUrl":"assets/api/v1/animation-library/mUlvnlbeZ5GHYr_Lb4NIuMwPs7kGxHWz/category_backgrounds/blank.png","frameSize":{"x":100,"y":100},"frameCount":1,"looping":true,"frameDelay":4,"version":"mUlvnlbeZ5GHYr_Lb4NIuMwPs7kGxHWz","loadedFromSource":true,"saved":true,"sourceSize":{"x":100,"y":100},"rootRelativePath":"assets/api/v1/animation-library/mUlvnlbeZ5GHYr_Lb4NIuMwPs7kGxHWz/category_backgrounds/blank.png"}}};
  var orderedKeys = animationListJSON.orderedKeys;
  var allAnimationsSingleFrame = false;
  orderedKeys.forEach(function (key) {
    var props = animationListJSON.propsByKey[key];
    var frameCount = allAnimationsSingleFrame ? 1 : props.frameCount;
    var image = loadImage(props.rootRelativePath, function () {
      var spriteSheet = loadSpriteSheet(
          image,
          props.frameSize.x,
          props.frameSize.y,
          frameCount
      );
      p5Inst._predefinedSpriteAnimations[props.name] = loadAnimation(spriteSheet);
      p5Inst._predefinedSpriteAnimations[props.name].looping = props.looping;
      p5Inst._predefinedSpriteAnimations[props.name].frameDelay = props.frameDelay;
    });
  });

  function wrappedExportedCode(stage) {
    if (stage === 'preload') {
      if (setup !== window.setup) {
        window.setup = setup;
      } else {
        return;
      }
    }
// -----

//variables
var count = 0;
var bananaGroup = createGroup();
var monkey = createSprite(100,310,50,50);
var ground = createSprite(200,380,400,20);
var invisibleGround = createSprite(200,380,400,20);
var obstacle = createSprite(340,340,10,40);
var banana = createSprite(340,230,10,40);
//velocity for ground
 ground.velocityX = -5;
//animation 
monkey.setAnimation("monkey");
ground.setAnimation("ground");
invisibleGround.setAnimation("invisibleGround");
obstacle.setAnimation("Stone1");
banana.setAnimation("banana1");
//Scale
monkey.scale = 0.2; 
obstacle.scale = 0.2;
banana.scale = 0.1 ;
//visible\non-visible
invisibleGround.visible = false; 
 
//making the obstacle to move
obstacle.velocityX = -7;

function draw() {
  background(255);
  console.log(banana.x);

  // keeping the ground to move countinusely
  if (ground.x < 0){
    ground.x = 400;
  }
  
    if (obstacle.x < 0){
    obstacle.x = 400;
  } 

spawnBanana();


    if( keyDown("space") && monkey.y >= 300){
    monkey.velocityY = -20;
    playSound("assets/category_jump/classic_jump_2.mp3", false);
  }
  
  if(monkey.isTouching(obstacle)){
    monkey.lifetime = 0;
    banana.lifetime = 0;
    obstacle.lifetime = 0;
    ground.lifetime = 0;
    var gameover = createSprite (200,200,20,20);
    gameover.setAnimation("gameOver");
    
  }

  if(monkey.isTouching(banana)){
//    banana.lifetime = 0;
    banana.visible = false;
    count = count+1;
  }

  //gravity for monkey
  monkey.velocityY = monkey.velocityY + 0.8;

// stop monkey from falling
  monkey.collide(invisibleGround); 

  drawSprites();
  text("Score :",200,100);
  text(count,250,100);
}

function spawnBanana(){
  if(World.frameCount % 60 === 0){
    bananaGroup.visible = true;
    playSound("assets/default.mp3", false);
    banana.velocityX = -5;
     if (banana.x < 200){
    banana.x = 400;
  }
  bananaGroup.add(banana);
  }
}


// -----
    try { window.draw = draw; } catch (e) {}
    switch (stage) {
      case 'preload':
        if (preload !== window.preload) { preload(); }
        break;
      case 'setup':
        if (setup !== window.setup) { setup(); }
        break;
    }
  }
  window.wrappedExportedCode = wrappedExportedCode;
  wrappedExportedCode('preload');
};

window.setup = function () {
  window.wrappedExportedCode('setup');
};