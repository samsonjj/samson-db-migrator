use std::fmt::Display;
use std::io::prelude::*;

use colored::Colorize;

pub fn print_iter<'a, T>(x: Box<dyn Iterator<Item = T> + 'a>)
where
    T: Display,
{
    println!("[");
    for (i, a) in x.enumerate() {
        if i == 0 {
            print!("    {}", a);
        } else {
            print!(",\n    {}", a);
        }
    }
    println!("\n]");
}

pub fn ask_password() -> String {
    println!("Enter db password:");
    std::io::stdout().flush().unwrap();
    rpassword::read_password().unwrap()
}
