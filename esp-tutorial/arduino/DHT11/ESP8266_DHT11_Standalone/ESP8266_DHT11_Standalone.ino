/*************************************************************
  Download latest Blynk library here:
    https://github.com/blynkkk/blynk-library/releases/latest

  Blynk is a platform with iOS and Android apps to control
  Arduino, Raspberry Pi and the likes over the Internet.
  You can easily build graphic interfaces for all your
  projects by simply dragging and dropping widgets.

    Downloads, docs, tutorials: http://www.blynk.cc
    Sketch generator:           http://examples.blynk.cc
    Blynk community:            http://community.blynk.cc
    Follow us:                  http://www.fb.com/blynkapp
                                http://twitter.com/blynk_app

  Blynk library is licensed under MIT license
  This example code is in public domain.

 *************************************************************

  This example shows how value can be pushed from Arduino to
  the Blynk App.

  WARNING :
  For this example you'll need Adafruit DHT sensor libraries:
    https://github.com/adafruit/Adafruit_Sensor
    https://github.com/adafruit/DHT-sensor-library

  App project setup:
    Value Display widget attached to V5
    Value Display widget attached to V6
 *************************************************************/

/* Comment this out to disable prints and save space */
/* #define BLYNK_PRINT Serial */


#include <ESP8266WiFi.h>
#include <BlynkSimpleEsp8266.h>
#include <DHT.h>

//// Use Blynk Static Provisioning since only expect 4 devices for my home micro weather station
// Auth Token for device 'Sta-1' : e5rGLloeOHCGjNomaR0KNbfHXSY1fVap
// Auth Token for device 'Sta-2' : a34l0ByU7ZeeQOrFla5lfbGqewr6mZI_
// Auth Token for device 'Sta-3' : CMg96HTDs7uTqv3LD6lmTSndMsIDCWgn
// Auth Token for device 'Sta-4' : L155ijUo03vRHYH1oasS3i-LYe0hCmcP

#define AUTH_STA1 "e5rGLloeOHCGjNomaR0KNbfHXSY1fVap"
#define AUTH_STA2 "a34l0ByU7ZeeQOrFla5lfbGqewr6mZI_"
#define AUTH_STA3 "CMg96HTDs7uTqv3LD6lmTSndMsIDCWgn"
#define AUTH_STA4 "L155ijUo03vRHYH1oasS3i-LYe0hCmcP"

char auth[] = AUTH_STA2;

// Use IOT Wifi Guest SSID on Asus

char ssid[] = "Oaks-IOT";
char pass[] = "acornnetwork";

#define DHTPIN 2          // ESP01S-DHT11 shield uses GPIO2 for sensor data

// Uncomment whatever type you're using!
#define DHTTYPE DHT11     // DHT 11
//#define DHTTYPE DHT22   // DHT 22, AM2302, AM2321
//#define DHTTYPE DHT21   // DHT 21, AM2301

// Initialize DHT sensor 
DHT dht(DHTPIN, DHTTYPE);

BlynkTimer timer;

// This function sends Arduino's up time every second to Virtual Pin (5).
// In the app, Widget's reading frequency should be set to PUSH. This means
// that you define how often to send data to Blynk App.
void sendSensor()
{
  float h = dht.readHumidity();
  float t = dht.readTemperature(); // or dht.readTemperature(true) for Fahrenheit

  if (isnan(h) || isnan(t)) {
    Serial.println("Failed to read from DHT sensor!");
    return;
  }
  // You can send any value at any time.
  // Please don't send more that 10 values per second.
  Blynk.virtualWrite(V5, h);
  Blynk.virtualWrite(V6, t);
}

void setup()
{
  // Debug console
  Serial.begin(9600);

  Blynk.begin(auth, ssid, pass);
  // You can also specify server:
  //Blynk.begin(auth, ssid, pass, "blynk-cloud.com", 80);
  //Blynk.begin(auth, ssid, pass, IPAddress(192,168,1,100), 8080);

  dht.begin();

  // Setup a function to be called every 10 seconds
  timer.setInterval(10000L, sendSensor);
}

void loop()
{
  Blynk.run();
  timer.run();
}
