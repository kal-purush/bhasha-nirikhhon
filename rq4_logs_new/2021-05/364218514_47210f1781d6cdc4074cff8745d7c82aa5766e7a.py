# 3.3D
// This is the code of pad
int led = D3; 
void setup() 
{
  pinMode(led, OUTPUT);
}
void loop() 
{
    if(Particle.publish("Deakin_RIOT_SIT210_Photon_Buddy", "pad")==true)
    {
        for(int i=0;i<3;i++)
        {
            digitalWrite(led, HIGH);
            delay(800);
            digitalWrite(led, LOW);
            delay(800);
            digitalWrite(led, HIGH);
            delay(800);
            digitalWrite(led, LOW);
            delay(800);
            
        }
    }
    delay(8000);
}

************************************
// This is the code of wave
int led = D3; 
void setup() 
{
  pinMode(led, OUTPUT);
}
void loop() 
{
    if(Particle.publish("Deakin_RIOT_SIT210_Photon_Buddy", "wave")==true)
    {
        for(int i=0;i<3;i++)
        {
            digitalWrite(led, HIGH);
            delay(900);
            digitalWrite(led, LOW);
            delay(900);
        }
    }
    delay(9000);