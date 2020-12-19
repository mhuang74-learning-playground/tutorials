/**
 * Blink
 * 
 * Turn on an LED for one second,
 * then off for another one second.
 * Then repeats.
*/

#include <Arduino.h>

#define LED_BUILTIN 2

void setup() {
  // initialize LED digital pin as output
  pinMode(LED_BUILTIN, OUTPUT);

  Serial.begin(115200);

}

int count=0;

void loop() {
  // print Count to Serial
  count = count+1;
  char msg[25];
  sprintf(msg, "Hello There! Count: %4d", count);
  Serial.println(msg);

  // turn LED on
  digitalWrite(LED_BUILTIN, HIGH);

  // wait 1 second
  delay(300);

  // turn LED off
  digitalWrite(LED_BUILTIN, LOW);

  // wait another second
  delay(500); 
}

