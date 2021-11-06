#[allow(unused_imports)]
use esp_idf_sys; // If using the `binstart` feature of `esp-idf-sys`, always keep this module imported

const KILOBYTE: usize = 1024;

fn test_memory_allocation() -> () {

    unsafe { esp_idf_sys::heap_caps_print_heap_info(esp_idf_sys::MALLOC_CAP_8BIT); }

    for i in (1..4000).step_by(32) {
        let size = i * KILOBYTE;
        println!("{}: allocating Vec<u8> of size: {}", i, size);
        let _new_vec: Vec<u8> = Vec::with_capacity(size);
    }
}

fn main() {
    // Temporary. Will disappear once ESP-IDF 4.4 is released, but for now it is necessary to call this function once,
    // or else some patches to the runtime implemented by esp-idf-sys might not link properly.
    esp_idf_sys::link_patches();

    println!("Hello, world!");

    test_memory_allocation();

    println!("That's all, folks!");
}
