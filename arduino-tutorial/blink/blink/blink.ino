#define ONBOARD_LED  2
 
void setup() {
  pinMode(ONBOARD_LED,OUTPUT);
  Serial.begin(115200);

   

}

int count=0;
   
void loop() {
  count = count+1;
  char hello[25];
  sprintf(hello, "Hello There! Count: %4d", count);
  Serial.println(hello);

  // print heap info
  char msg[64];
  sprintf(msg, "Total heap: %d", ESP.getHeapSize());
  Serial.println(msg);

  sprintf(msg, "Free heap: %d", ESP.getFreeHeap());
  Serial.println(msg);

  sprintf(msg, "Total PSRAM: %d", ESP.getPsramSize());
  Serial.println(msg);  

  sprintf(msg, "Free PSRAM: %d", ESP.getFreePsram());
  Serial.println(msg);
  
  
  delay(1000);
  digitalWrite(ONBOARD_LED,HIGH);
  delay(300);
  digitalWrite(ONBOARD_LED,LOW);
  

}
