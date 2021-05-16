use std::io;
use rand::Rng;
use std::cmp::Ordering;

fn main() {
    println!("Guess the number!");

    let secrete_number = rand::thread_rng().gen_range(0..255);

 //   println!("The secrete number is: {}", secrete_number);

    loop {

	    println!("Please input your guess (between 0-255)");

	    let mut guess = String::new();

	    io::stdin()
	    	.read_line(&mut guess)
	    	.expect("Failed to read line");

	    let guess: u8 = match guess.trim().parse() {
	    	Ok(num) => num,
	    	Err(_) => {
	    		println!("Please enter a number! (between 0-255)");
	    		continue;
	    	}

	    };
							

	    println!("You guessed: {}", guess);

	    match guess.cmp(&secrete_number) {
	    	Ordering::Less => println!("Too small!"),
	    	Ordering::Greater => println!("Too big!"),
	    	Ordering::Equal => {
	    		println!("You win!");
	    		break;
	    	}
	    }

	}
}
