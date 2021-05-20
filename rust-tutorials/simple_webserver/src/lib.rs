use std::thread;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPool {
	// our closure passed to thread::spawn() won't return anything,
	// so return type of unit type () is used by JoinHandle<T>
	workers: Vec<Worker>,
	sender: mpsc::Sender<Job>,
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

		for idx in 0..size {
			workers.push(Worker::new(idx, Arc::clone(&rx)));
		}

		ThreadPool { workers, sender:tx }
	}

	pub fn execute<F>(&self, f: F) 
	where
		F: FnOnce() + Send + 'static,
	{

		let job = Box::new(f);

		self.sender.send(job).unwrap();

	}
}

struct Worker {
	id: usize,
	thread: thread::JoinHandle<()>,
}

impl Worker {
	/// Create a new Worker.
	///
	fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {

		// create new thread with empty closure
		let thread = thread::spawn(move || loop {
			let job = receiver.lock().unwrap().recv().unwrap();

			println!("Worker {} got a job; executing.", id);

			job();
		});

		Worker { id, thread }
	}
}


