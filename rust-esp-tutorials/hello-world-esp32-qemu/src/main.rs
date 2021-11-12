#[allow(unused_imports)]
use esp_idf_sys; // If using the `binstart` feature of `esp-idf-sys`, always keep this module imported
use esp_idf_hal;


fn test_memory_allocation(kb_blocks:usize, step:usize) -> () {
    const KILOBYTE: usize = 1024;

    for i in (step..=kb_blocks).step_by(step) {
        let size = i * KILOBYTE;
        println!("{}: allocating Vec<u8> of size: {}", i, size);
        let mut new_vec: Vec<u8> = Vec::with_capacity(size);
        for j in 1..=size {
            new_vec.push(j as u8);
        }
    }

    println!("Allocated {:?} KB blocks in step of {:?}.", &kb_blocks, &step);

    unsafe { esp_idf_sys::heap_caps_print_heap_info(esp_idf_sys::MALLOC_CAP_8BIT); }
}

fn main() {
    // Temporary. Will disappear once ESP-IDF 4.4 is released, but for now it is necessary to call this function once,
    // or else some patches to the runtime implemented by esp-idf-sys might not link properly.
    esp_idf_sys::link_patches();

    println!("Hello, world!");

    test_memory_allocation(1024, 64);

    for i in (1..=10).rev() {
        println!("Restarting in {} seconds...", i);
        unsafe { esp_idf_sys::vTaskDelay(1000 / esp_idf_hal::delay::portTICK_PERIOD_MS); }
    }
    println!("Restarting now.\n");
    unsafe {esp_idf_sys::esp_restart(); }

}
