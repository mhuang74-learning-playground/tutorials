/* Simple LED PWM Example
   Author: Michael S. Huang
*/
#include <stdio.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "driver/ledc.h"
#include "esp_err.h"
#include "esp_system.h"
#include "esp_spi_flash.h"

#define LEDC_HS_TIMER          LEDC_TIMER_0
#define LEDC_HS_MODE           LEDC_HIGH_SPEED_MODE

// esp32-cam LED on GPIO33
#define LEDC_HS_CH0_GPIO       (33)
#define LEDC_HS_CH0_CHANNEL    LEDC_CHANNEL_0

#define LEDC_TEST_CH_NUM       (1)
#define LEDC_TEST_DUTY         (256)
#define LEDC_TEST_FADE_TIME    (3000)

void init(void) 
{
    printf("==== ESP32 CHIP INFO ====\n");

    /* Print chip information */
    esp_chip_info_t chip_info;
    esp_chip_info(&chip_info);

    printf("This is %s chip with %d CPU cores, WiFi%s%s, ",
            CONFIG_IDF_TARGET,
            chip_info.cores,
            (chip_info.features & CHIP_FEATURE_BT) ? "/BT" : "",
            (chip_info.features & CHIP_FEATURE_BLE) ? "/BLE" : "");

    printf("silicon revision %d, ", chip_info.revision);

    printf("%dMB %s flash\n", spi_flash_get_chip_size() / (1024 * 1024),
            (chip_info.features & CHIP_FEATURE_EMB_FLASH) ? "embedded" : "external");

    printf("Free heap: %d\n", esp_get_free_heap_size());


}

/*
    * Prepare and set configuration of timers
    * that will be used by LED Controller
    */
ledc_timer_config_t ledc_timer = {
    .duty_resolution = LEDC_TIMER_8_BIT, // resolution of PWM duty
    .freq_hz = 4000,                      // frequency of PWM signal
    .speed_mode = LEDC_HS_MODE,           // timer mode
    .timer_num = LEDC_HS_TIMER,            // timer index
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
        .channel    = LEDC_HS_CH0_CHANNEL,
        .duty       = 0,
        .gpio_num   = LEDC_HS_CH0_GPIO,
        .speed_mode = LEDC_HS_MODE,
        .hpoint     = 0,
        .timer_sel  = LEDC_HS_TIMER
    },
};

int ch;


void setup(void) {
    // Set configuration of timer0 for high speed channels
    ledc_timer_config(&ledc_timer);


    // Set LED Controller with previously prepared configuration
    for (ch = 0; ch < LEDC_TEST_CH_NUM; ch++) {
        ledc_channel_config(&ledc_channel[ch]);
    }
}

void app_main(void)
{

    init();

    setup();


    int duty = 0;

    while (1) {


        for (duty = 0; duty < LEDC_TEST_DUTY; duty++) {
            printf("LED UP: set duty = %d\n", duty);

            for (ch = 0; ch < LEDC_TEST_CH_NUM; ch++) {
                ledc_set_duty(ledc_channel[ch].speed_mode, ledc_channel[ch].channel, duty);
                ledc_update_duty(ledc_channel[ch].speed_mode, ledc_channel[ch].channel);
            }

            vTaskDelay(10 / portTICK_PERIOD_MS);
        }

        vTaskDelay(100 / portTICK_PERIOD_MS);

        for (duty = LEDC_TEST_DUTY; duty > 0; duty--) {
            printf("LED DOWN: set duty = %d\n", duty);

            for (ch = 0; ch < LEDC_TEST_CH_NUM; ch++) {
                ledc_set_duty(ledc_channel[ch].speed_mode, ledc_channel[ch].channel, duty);
                ledc_update_duty(ledc_channel[ch].speed_mode, ledc_channel[ch].channel);
            }

            vTaskDelay(10 / portTICK_PERIOD_MS);
        }

    }
}
