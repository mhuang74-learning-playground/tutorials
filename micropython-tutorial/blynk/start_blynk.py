# -*- coding: utf-8 -*-
"""
Basic Blynk Connect Loop
"""
import blynklib_mp as blynklib
import network
import utime as time
import machine


WIFI_SSID = 'Oaks-IOT'
WIFI_PASS = 'acornnetwork'
BLYNK_AUTH = 'ofk-wfBKXNicjrv4HOELdifVt6dAhGJI'


print("Connecting to WiFi network '{}'".format(WIFI_SSID))
wifi = network.WLAN(network.STA_IF)
wifi.active(True)
wifi.connect(WIFI_SSID, WIFI_PASS)
while not wifi.isconnected():
    time.sleep(1)
    print('WiFi connect retry ...')
print('WiFi IP:', wifi.ifconfig()[0])

print("Connecting to Blynk server...")
blynk = blynklib.Blynk(BLYNK_AUTH)


def runLoop():
    while True:
        blynk.run()
        machine.idle()

# Run blynk in the main thread:
runLoop()

