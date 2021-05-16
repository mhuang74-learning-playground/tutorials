#define ONBOARD_LED  2
 
void setup() {
  pinMode(ONBOARD_LED,OUTPUT);
  Serial.begin(9600);
}

int count=0;
   
void loop() {
  count = count+1;
  char msg[25];
  sprintf(msg, "Hello There! Count: %4d", count);
  Serial.println(msg);
  
  delay(1000);
  digitalWrite(ONBOARD_LED,HIGH);
  delay(300);
  digitalWrite(ONBOARD_LED,LOW);
  

}
