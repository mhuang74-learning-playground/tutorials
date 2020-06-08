#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "driver/ledc.h"
#include "esp_err.h"
#include "esp_system.h"
#include "esp_spi_flash.h"

#include "../output_handler.h"

#define LEDC_HS_TIMER          LEDC_TIMER_0
#define LEDC_HS_MODE           LEDC_HIGH_SPEED_MODE

#define LEDC_HS_CH0_GPIO       (2)
#define LEDC_HS_CH0_CHANNEL    LEDC_CHANNEL_0

#define LEDC_TEST_CH_NUM       (1)
#define LEDC_TEST_DUTY         (256)
#define LEDC_TEST_FADE_TIME    (3000)

/*
    * Prepare and set configuration of timers
    * that will be used by LED Controller
    */
ledc_timer_config_t ledc_timer = {
    .speed_mode = LEDC_HS_MODE,           // timer mode
    .duty_resolution = LEDC_TIMER_8_BIT, // resolution of PWM duty
    .timer_num = LEDC_HS_TIMER,            // timer index
    .freq_hz = 4000,                      // frequency of PWM signal
    .clk_cfg = LEDC_AUTO_CLK,              // Auto select the source clock
};

/*
    * Prepare individual configuration
    * for each channel of LED Controller
    * by selecting:
    * - controller's channel number
    * - output duty cycle, set initially to 0
    * - GPIO number where LED is connected to
    * - speed mode, either high or low
    * - timer servicing selected channel
    *   Note: if different channels use one timer,
    *         then frequency and bit_num of these channels
    *         will be the same
    */
ledc_channel_config_t ledc_channel[LEDC_TEST_CH_NUM] = {
    {
        .gpio_num   = LEDC_HS_CH0_GPIO,
        .speed_mode = LEDC_HS_MODE,
        .channel    = LEDC_HS_CH0_CHANNEL,
        .intr_type  = LEDC_INTR_DISABLE,
        .timer_sel  = LEDC_HS_TIMER,        
        .duty       = 0,
        .hpoint     = 0,
    },
};

int ch = 0;

void init_ledc(void)
{


    // Set configuration of timer0 for high speed channels
    ledc_timer_config(&ledc_timer);

    // Set LED Controller with previously prepared configuration
    for (ch = 0; ch < LEDC_TEST_CH_NUM; ch++) {
        ledc_channel_config(&ledc_channel[ch]);
    }

}

void set_ledc(int duty) {

    for (ch = 0; ch < LEDC_TEST_CH_NUM; ch++) {
        ledc_set_duty(ledc_channel[ch].speed_mode, ledc_channel[ch].channel, duty);
        ledc_update_duty(ledc_channel[ch].speed_mode, ledc_channel[ch].channel);
    }

    vTaskDelay(10 / portTICK_PERIOD_MS);
}

// Track whether the function has run at least once
bool initialized = false;

// Animates a dot across the screen to represent the current x and y values
void HandleOutput(tflite::ErrorReporter* error_reporter, float x_value,
                  float y_value) {
  // Do this only once
  if (!initialized) {
    // Set the LED pin to output
    init_ledc();
    initialized = true;
  }

  // Sine Model: Calculate the brightness of the LED such that y=-1 is fully off
  // and y=1 is fully on. The LED's brightness can range from 0-255.
  // int brightness = (int)(127.5f * (y_value + 1));

  // Hyperbolic Sine Model: just convert to int
  int brightness = (int)(y_value);

  // Set the brightness of the LED. If the specified pin does not support PWM,
  // this will result in the LED being on when y > 127, off otherwise.
  set_ledc(brightness);

  // Log the current brightness value for display in the Arduino plotter
  TF_LITE_REPORT_ERROR(error_reporter, "y_value = %f, Duty = %d", y_value, brightness);
}