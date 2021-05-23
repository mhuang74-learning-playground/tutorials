use std::thread;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;

type Job = Box<dyn FnOnce() + Send + 'static>;

enum Message {
	NewJob(Job),
	Terminate
}

pub struct ThreadPool {
	// our closure passed to thread::spawn() won't return anything,
	// so return type of unit type () is used by JoinHandle<T>
	workers: Vec<Worker>,
	sender: mpsc::Sender<Message>,
}

impl ThreadPool {
	/// Create a new ThreadPool.
	///
	/// The size is the number of threads in the pool.
	///
	/// # Panics
	///
	/// The `new` function will panic if the size is zero.
	pub fn new(size: usize) -> ThreadPool {
		assert!(size > 0);

		let mut workers = Vec::with_capacity(size);

		let (tx,rx) = mpsc::channel();

		let rx = Arc::new(Mutex::new(rx));

		for idx in 1..size+1 {
			workers.push(Worker::new(idx, Arc::clone(&rx)));
		}


		ThreadPool { workers, sender:tx }	
	}

	pub fn execute<F>(&self, f: F) 
	where
		F: FnOnce() + Send + 'static,
	{

		let job = Box::new(f);

		self.sender.send(Message::NewJob(job)).unwrap();

		println!("Request sent to Worker channel");

	}

	pub fn terminate(&mut self) {

		println!("Sending terminate message to all workers.");

		// send any many Terminate msssage as there are Workers
		for _ in &self.workers {
			self.sender.send(Message::Terminate).unwrap();
		}

		println!("Shutting down all workers.");

		for worker in &mut self.workers {
			println!("Shutting down worker {}", worker.id);

			if let Some(thread) = worker.thread.take() {
				thread.join().unwrap();
			}
		}
	}	
}

impl Drop for ThreadPool {
	fn drop(&mut self) {
		println!("Dropping off..bye bye!")
	}
}

struct Worker {
	id: usize,
	thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
	/// Create a new Worker.
	///
	fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {

		// create new thread with empty closure
		let thread = thread::spawn(move || loop {

			let message = receiver.lock().unwrap().recv().unwrap();

			match message {
				Message::NewJob(job) => {

					println!("Worker {} received new job; executing.", id);

					job();
				}
				Message::Terminate => {

					println!("Worker {} was told to TERMINATE.", id);

					break;

				}
			}

		});

		Worker { id, thread: Some(thread) }
	}
}


