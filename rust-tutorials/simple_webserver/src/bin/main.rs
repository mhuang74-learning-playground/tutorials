use std::io::prelude::*;
use std::net::TcpListener;
use std::net::TcpStream;
use std::fs;
use std::thread;
use std::time::Duration;
use simple_webserver::ThreadPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;


fn main() {
	const SERVER_ADDRESS : &str = "127.0.0.1:7878";

	println!("Starting Simple Webserver at {}...", SERVER_ADDRESS);

	let listener = TcpListener::bind(SERVER_ADDRESS).unwrap();

	let mut pool = ThreadPool::new(4);

	let shutdown = Arc::new(AtomicBool::new(false));
	let s = shutdown.clone();

	//  Ctrl-C sends Termination
	ctrlc::set_handler(move || {

		s.store(true, Ordering::SeqCst);

		println!("Ctrl-C detected. Shutdown flag set.");

	}).expect("Error setting Ctrl-C handler");

	// equivalent to calling listner.accept() in a loop
	// accept() blocks caller until new TCP connection is established
	for stream in listener.incoming() {

		// check if need terminate first
		if shutdown.load(Ordering::SeqCst) {
			println!("Commencing shutdown...");

			pool.terminate();

			break;
		} 

		let stream = stream.unwrap();

		pool.execute(|| {
			handle_connection(stream);
		});


	}



    fn handle_connection(mut stream: TcpStream) {

    	let mut buffer = [0; 1024];

    	stream.read(&mut buffer).unwrap();

	    // println!("Thread: {:?} - Request: {}", thread::current().id(), String::from_utf8_lossy(&buffer[..]));
		println!("Thread: {:?} - Got Request", thread::current().id());

    	let get = b"GET / HTTP/1.1\r\n";
    	let sleep = b"GET /sleep HTTP/1.1\r\n";

    	let (status_line, filename) = 
	    	if buffer.starts_with(get) {
	    		("HTTP/1.1 200 OK", "hello.html")
	    	} else if buffer.starts_with(sleep) {
	    		thread::sleep(Duration::from_secs(1));
	    		("HTTP/1.1 200 OK", "hello.html")
	    	} else {
	    		("HTTP/1.1 404 NOT FOUND", "404.html")
	    	}
    	;

		let contents = fs::read_to_string(filename).unwrap();

    	let response = format!(
    		"{}\r\nContent-Length: {}\r\n\r\n{}",
    		status_line,
    		contents.len(),
    		contents
		);

    	stream.write(response.as_bytes()).unwrap();
    	stream.flush().unwrap(); 	

    }
}
